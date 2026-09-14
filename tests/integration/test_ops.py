"""Ops summary and cost report against a LocalStack fixture with known contents."""

from __future__ import annotations

import threading
import time

import httpx
import pytest
from conduit.core.idempotency import idempotency_key
from conduit.core.queue import ensure_queues
from conduit.core.schema import SchemaField, SourceSchema
from conduit.models import Envelope
from conduit.ops import StatusStore, collect, render_costs, render_summary, usd
from conduit.worker import QUEUE_DEPTH_INTERVAL_SECONDS
from prometheus_client import start_http_server
from prometheus_client.parser import text_string_to_metric_families

from tests.integration.conftest import _free_port, task

pytestmark = pytest.mark.integration


def seeded(rig, statuses: StatusStore):
    """Three delivered, one dead-lettered, one quarantined, one still queued."""
    rig.fake.faults(hard_fail_tasks=["OPS-dead"])
    rig.submit(*[task(f"OPS-{i}", fields={"region": "emea"}) for i in range(3)])
    rig.submit(task("OPS-dead", fields={"region": "emea"}), task("OPS-bad"))
    rig.worker.statuses = statuses
    rig.run(idle_polls=3)
    rig.submit(task("OPS-waiting", fields={"region": "emea"}))
    return rig


@pytest.fixture
def ops_rig(rig_factory, store):
    schema = SourceSchema(
        connector="ops",
        version=1,
        fields={"fields.region": SchemaField(type="string", required=True)},
    )
    rig = rig_factory("webhook-crm", schema=schema)
    statuses = StatusStore(store.table_name, client=store._client)
    return seeded(rig, statuses), statuses


def row_for(rows, name):
    return next(r for r in rows if r.name == name)


def test_summary_reports_the_exact_depths_and_worker_state(ops_rig, aws, specs):
    rig, statuses = ops_rig
    rows = collect({rig.spec.name: rig.spec}, aws["sqs"], statuses)
    row = row_for(rows, rig.spec.name)

    assert row.type == "webhook"
    assert row.work.visible == 1
    assert row.dlq.total == 1
    assert row.quarantine.total == 1

    status = row.status
    assert status is not None
    assert status.delivered == 3
    assert status.quarantined == 1
    assert status.dead_lettered == 1
    assert status.failed == 2
    assert status.received == 6
    assert status.breaker_state == "closed"
    assert status.last_error.startswith("permanent OPS-dead")
    assert status.run_id == rig.worker.run_id


def test_summary_renders_one_line_per_connector_with_those_values(ops_rig, aws):
    rig, statuses = ops_rig
    rows = collect({rig.spec.name: rig.spec}, aws["sqs"], statuses)
    rendered = render_summary(rows).splitlines()

    assert rendered[0].split() == [
        "connector",
        "queue",
        "inflight",
        "dlq",
        "quar",
        "delivered",
        "per",
        "min",
        "throttle",
        "breaker",
        "last",
        "error",
    ]
    body = rendered[2]
    assert body.startswith(rig.spec.name)
    assert body.split()[1:6] == ["1", "0", "1", "1", "3"]
    assert "closed" in body and "tok" in body
    assert rendered[-1].split() == ["total", "1", "0", "1", "1", "3"]


def test_costs_report_counts_written_during_the_run(ops_rig, aws):
    rig, statuses = ops_rig
    rows = collect({rig.spec.name: rig.spec}, aws["sqs"], statuses)
    status = row_for(rows, rig.spec.name).status

    assert status.remote_objects == status.delivered == 3
    # Nine writes for the three deliveries (claim, delivered mark, latest pointer)
    # and four for OPS-dead, which is claimed and released once per receive. The
    # quarantined payload never reaches the store. Reads are the get_item each
    # delivery does to find the connector for its latest pointer.
    assert status.dynamodb_writes == 3 * 3 + 2 * 2 == 13
    assert status.dynamodb_reads == 3
    assert status.sqs_requests > 0

    rendered = render_costs(rows).splitlines()
    assert rendered[0].split() == [
        "connector",
        "run",
        "objects",
        "sqs",
        "req",
        "ddb",
        "write",
        "ddb",
        "read",
        "usd",
    ]
    body = rendered[2].split()
    assert body[0] == rig.spec.name and body[1] == rig.worker.run_id
    assert body[2] == "3" and body[4] == "13" and body[5] == "3"
    assert rendered[-1].startswith("priced at us-east-1 on-demand list")


def test_cost_arithmetic_is_the_documented_rate_table():
    counts = {"sqs_requests": 1_000_000, "dynamodb_writes": 1_000_000, "dynamodb_reads": 1_000_000}
    assert usd(counts) == pytest.approx(0.40 + 1.25 + 0.25)
    assert usd({}) == 0.0


def test_a_connector_with_no_worker_and_no_queues_still_renders(aws, specs, store):
    spec = specs["slack-ops"].model_copy(update={"name": "never-started"})
    statuses = StatusStore(store.table_name, client=store._client)
    rows = collect({spec.name: spec}, aws["sqs"], statuses)

    assert rows[0].status is None and rows[0].work.total == 0
    assert "no worker seen" in render_summary(rows)
    assert "no worker seen" in render_costs(rows)


def test_status_survives_a_round_trip_through_dynamodb(ops_rig, store):
    rig, statuses = ops_rig
    written = rig.worker.status()
    statuses.publish(written)
    read = statuses.read(rig.spec.name)
    assert read == written


def test_queues_created_by_ensure_are_all_three(rig_factory, aws):
    rig = rig_factory("slack-ops")
    queue, dlq, quarantine = ensure_queues(rig.spec, aws["sqs"])
    assert queue.name == rig.spec.queue_name
    assert dlq.name == rig.spec.dlq_name
    assert quarantine.name == rig.spec.quarantine_name


def scraped_depths(port: int, connector: str) -> dict[tuple[str, str], float]:
    text = httpx.get(f"http://127.0.0.1:{port}/metrics", timeout=5).text
    return {
        (sample.labels["queue"], sample.labels["visibility"]): sample.value
        for family in text_string_to_metric_families(text)
        if family.name == "conduit_queue_depth"
        for sample in family.samples
        if sample.labels.get("connector") == connector
    }


def wait_for_depths(port, connector, ready, timeout):
    deadline = time.monotonic() + timeout
    while True:
        depths = scraped_depths(port, connector)
        if ready(depths) or time.monotonic() >= deadline:
            return depths
        time.sleep(0.5)


def test_a_running_worker_serves_fresh_queue_depth_on_metrics(rig_factory):
    schema = SourceSchema(
        connector="ops",
        version=1,
        fields={"fields.region": SchemaField(type="string", required=True)},
    )
    rig = rig_factory("webhook-crm", schema=schema)
    name = rig.spec.name
    port = _free_port()
    server, _ = start_http_server(port, addr="127.0.0.1")
    stop = threading.Event()
    runner = threading.Thread(
        target=rig.worker.run, kwargs={"stop": stop, "wait_seconds": 1}, daemon=True
    )
    runner.start()
    try:
        before = wait_for_depths(port, name, lambda d: ("dlq", "visible") in d, timeout=15)
        assert before[("dlq", "visible")] == 0
        assert before[("quarantine", "visible")] == 0

        # The worker never reads its DLQ, so these three stay put until the gauge sees them,
        # and the payload without fields.region is quarantined by the worker itself.
        dead = [task(f"DEPTH-dead-{i}") for i in range(3)]
        rig.dlq.send_batch(
            Envelope(task=t, connector=name, idempotency_key=idempotency_key(name, t.id, 1))
            for t in dead
        )
        rig.submit(task("DEPTH-bad"))

        after = wait_for_depths(
            port,
            name,
            lambda d: d.get(("dlq", "visible")) == 3 and d.get(("quarantine", "visible")) == 1,
            timeout=QUEUE_DEPTH_INTERVAL_SECONDS + 15,
        )
        assert after[("dlq", "visible")] == 3
        assert after[("quarantine", "visible")] == 1
        assert after[("work", "visible")] == 0
        assert rig.worker.stats.quarantined == 1
    finally:
        stop.set()
        runner.join(timeout=15)
        server.shutdown()
