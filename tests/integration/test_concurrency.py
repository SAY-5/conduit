"""Two workers on one queue over one DynamoDB table: each key is delivered once."""

from __future__ import annotations

import threading
import time

import pytest
from conduit.adapters import build_adapter
from conduit.config import RateLimit
from conduit.core.idempotency import DynamoIdempotencyStore, idempotency_key
from conduit.worker import Worker

from tests.integration.conftest import task

pytestmark = pytest.mark.integration


def wait_until(predicate, timeout: float, interval: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_two_workers_on_one_queue_deliver_each_key_once(rig_factory, store):
    rig = rig_factory("slack-ops", rate_limit=RateLimit(requests_per_second=100, burst=10))
    second = Worker(
        rig.spec,
        build_adapter(rig.spec),
        store,
        rig.queue,
        quarantine=rig.quarantine,
        dlq=rig.dlq,
    )
    workers = (rig.worker, second)
    stop = threading.Event()
    threads = [
        threading.Thread(target=w.run, kwargs={"stop": stop, "wait_seconds": 1}, daemon=True)
        for w in workers
    ]
    for thread in threads:
        thread.start()
    unique = [task(f"CC-{i}") for i in range(20)]
    rig.submit(*unique, *unique[:10])

    # A resubmit received while its original is still in progress is re-polled
    # after IN_PROGRESS_RECHECK_SECONDS, so the dedup count can lag the inbox.
    settled = wait_until(
        lambda: (
            rig.fake.inbox()["unique_keys"] == 20
            and sum(w.stats.deduplicated for w in workers) == 10
        ),
        timeout=60,
    )
    stop.set()
    for thread in threads:
        thread.join(timeout=15)

    inbox = rig.fake.inbox()
    assert settled, [w.summary() for w in workers]
    assert inbox["count"] == 20 and inbox["unique_keys"] == 20
    assert sum(w.stats.delivered for w in workers) == 20
    assert sum(w.stats.received for w in workers) >= 30
    assert all(w.stats.failed == 0 and w.stats.dead_lettered == 0 for w in workers)


def test_a_key_in_progress_elsewhere_is_rechecked_then_deduplicated(rig_factory, store):
    rig = rig_factory("slack-ops")
    key = idempotency_key(rig.spec.name, "IP-1", 1)
    assert store.claim(key, connector=rig.spec.name, task_id="IP-1").acquired
    rig.submit(task("IP-1"))

    rig.worker.run(max_messages=1, wait_seconds=2)
    assert rig.worker.stats.received == 1 and rig.worker.stats.delivered == 0
    assert rig.fake.inbox()["count"] == 0

    store.mark_delivered(key, "1700000000.000100")
    stats = rig.worker.run(idle_polls=8, wait_seconds=2)
    assert stats.deduplicated == 1 and stats.delivered == 0
    assert rig.fake.inbox()["count"] == 0


def test_an_abandoned_claim_is_taken_over_after_its_lease(rig_factory, store, aws):
    rig = rig_factory("slack-ops")
    dying = DynamoIdempotencyStore(store.table_name, client=aws["dynamodb"], lease_seconds=2)
    key = idempotency_key(rig.spec.name, "AB-1", 1)
    assert dying.claim(key, connector=rig.spec.name, task_id="AB-1").acquired
    rig.submit(task("AB-1"))
    time.sleep(2.2)

    stats = rig.worker.run(max_messages=1, wait_seconds=2)
    assert stats.delivered == 1 and stats.deduplicated == 0
    inbox = rig.fake.inbox()
    assert inbox["count"] == 1 and inbox["entries"][0]["task_id"] == "AB-1"
