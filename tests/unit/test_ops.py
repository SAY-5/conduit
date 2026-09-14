"""Ops rendering and the status row round trip, without AWS."""

from __future__ import annotations

import pytest
from conduit.ops import (
    ConnectorOps,
    Depth,
    StatusStore,
    WorkerStatus,
    render_costs,
    render_summary,
    status_pk,
    usd,
)


class FakeDynamo:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def put_item(self, *, TableName, Item):  # noqa: N803 - boto3 spelling
        self.items[Item["pk"]["S"]] = Item

    def get_item(self, *, TableName, Key, ConsistentRead=False):  # noqa: N803
        item = self.items.get(Key["pk"]["S"])
        return {"Item": item} if item else {}


def status(**overrides) -> WorkerStatus:
    base = {
        "connector": "slack-ops",
        "run_id": "R1",
        "started_at": 1000.0,
        "updated_at": 1060.0,
        "delivered": 30,
    }
    return WorkerStatus(**{**base, **overrides})


def row(**overrides) -> ConnectorOps:
    base = {
        "name": "slack-ops",
        "type": "slack",
        "work": Depth(visible=4, in_flight=1),
        "dlq": Depth(visible=2),
        "quarantine": Depth(visible=3),
        "status": status(),
    }
    return ConnectorOps(**{**base, **overrides})


def test_throughput_is_delivered_per_minute_of_the_run():
    assert status().per_minute() == 30.0
    assert status(updated_at=1000.0).per_minute() == 0.0
    assert status(updated_at=1030.0).per_minute() == 60.0


def test_summary_line_carries_every_depth_and_the_worker_state():
    line = render_summary([row(status=status(breaker_state="open", tokens_available=2.5))])
    body = line.splitlines()[2]
    assert body.split()[:8] == ["slack-ops", "4", "1", "2", "3", "30", "30.0", "2.5"]
    assert "open" in body


def test_summary_totals_add_every_queue():
    rendered = render_summary([row(), row(name="jira-support")]).splitlines()
    assert rendered[-1].split() == ["total", "8", "2", "4", "6", "60"]


def test_a_connector_without_a_status_reads_no_worker_seen():
    rendered = render_summary([row(status=None)]).splitlines()[2]
    assert rendered.endswith("no worker seen")
    assert rendered.split()[5] == "0"


def test_costs_line_reports_the_run_and_its_counts():
    counted = status(sqs_requests=400, dynamodb_writes=90, dynamodb_reads=30, remote_objects=30)
    body = render_costs([row(status=counted)]).splitlines()[2]
    assert body.split()[:6] == ["slack-ops", "R1", "30", "400", "90", "30"]


def test_costs_price_each_unit_at_the_documented_rate():
    assert usd({"sqs_requests": 2_000_000}) == pytest.approx(0.80)
    assert usd({"dynamodb_writes": 400_000}) == pytest.approx(0.50)
    assert usd({"remote_objects": 10_000_000}) == 0.0


def test_status_round_trips_through_the_table():
    fake = FakeDynamo()
    store = StatusStore("t", client=fake, clock=lambda: 500.0)
    written = status(last_error="permanent T-1: HTTP 400", tokens_available=1.25)
    store.publish(written)

    assert status_pk("slack-ops") in fake.items
    assert fake.items[status_pk("slack-ops")]["expires_at"]["N"] == "4100"
    assert store.read("slack-ops") == written


def test_reading_a_connector_with_no_row_is_none():
    assert StatusStore("t", client=FakeDynamo()).read("nobody") is None
