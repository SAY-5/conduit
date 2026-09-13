"""Throttling and the breaker pause against real SQS visibility timeouts (LocalStack)."""

from __future__ import annotations

import threading
import time

import pytest
from conduit.config import BreakerSpec, QueueSpec, RateLimit, RetryPolicy
from conduit.core.breaker import BreakerState
from conduit.core.queue import list_dead_letters

from tests.integration.conftest import task

pytestmark = pytest.mark.integration

NO_IN_PROCESS_RETRY = RetryPolicy(max_attempts=1, base_seconds=0.01, max_seconds=0.05)


def wait_until(predicate, timeout: float, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def busiest_window(stamps: list[float], width: float) -> int:
    """Most arrivals in any window of ``width`` seconds."""
    ordered = sorted(stamps)
    return max(sum(1 for later in ordered if start <= later < start + width) for start in ordered)


def test_throttle_holds_the_configured_rate_over_a_window(rig_factory):
    rate, burst, sends = 5.0, 1, 6
    rig = rig_factory("slack-ops", rate_limit=RateLimit(requests_per_second=rate, burst=burst))
    rig.submit(*[task(f"TB-{i}") for i in range(sends)])
    stats = rig.worker.run(max_messages=sends, wait_seconds=2)

    assert stats.delivered == sends
    assert stats.rate_limit_waits > 0
    stamps = sorted(e["received_at"] for e in rig.fake.inbox()["entries"])
    assert len(stamps) == sends
    # Tokens are capped at burst, so by any time t the remote has seen at most
    # burst + rate * elapsed sends. That bound holds however the polling happens
    # to be scheduled, unlike the count of waits, which drops when a slow poll
    # leaves the bucket time to refill.
    assert stamps[-1] - stamps[0] >= (sends - burst) / rate - 0.02
    assert busiest_window(stamps, 1.0) <= burst + rate


def test_retry_after_pauses_the_connector_not_just_the_message(rig_factory):
    rig = rig_factory("jira-support", rate_limit=RateLimit(requests_per_second=100, burst=10))
    rig.fake.faults(rate_limit_tasks=["RA-1"], rate_limit_count=1)
    rig.submit(task("RA-1"), task("RA-2"))
    stats = rig.run()

    assert stats.delivered == 2 and stats.retried == 1
    assert stats.retry_after_honored == 1
    assert rig.fake.inbox()["count"] == 2


def test_a_failing_downstream_pauses_the_source_then_it_resumes(rig_factory):
    rig = rig_factory(
        "slack-ops",
        retry=NO_IN_PROCESS_RETRY,
        breaker=BreakerSpec(failure_threshold=2, recovery_seconds=1.0),
        queue=QueueSpec(max_receive_count=20, visibility_timeout_seconds=2),
    )
    rig.fake.faults(outage=True)
    rig.submit(task("PR-1"), task("PR-2"), task("PR-3"))

    rig.worker.run(max_messages=2, wait_seconds=2)
    assert rig.worker.breaker.state is BreakerState.OPEN
    assert rig.worker.stats.breaker_opens == 1
    assert rig.worker.stats.delivered == 0 and rig.fake.inbox()["count"] == 0

    calls_when_paused = rig.fake.client.get("/_faults").json()["calls"]
    stop = threading.Event()
    thread = threading.Thread(
        target=rig.worker.run, kwargs={"stop": stop, "wait_seconds": 1}, daemon=True
    )
    thread.start()
    time.sleep(0.5)
    assert rig.fake.client.get("/_faults").json()["calls"] == calls_when_paused

    rig.fake.clear_faults()
    delivered = wait_until(lambda: rig.fake.inbox()["count"] == 3, timeout=30)
    stop.set()
    thread.join(timeout=15)

    assert delivered, rig.worker.summary()
    assert rig.worker.breaker.state is BreakerState.CLOSED
    assert rig.worker.stats.breaker_paused_seconds > 0
    inbox = rig.fake.inbox()
    assert inbox["count"] == 3 and inbox["unique_keys"] == 3
    assert list_dead_letters(rig.dlq) == []


def test_a_held_message_is_not_redelivered_during_a_pause(rig_factory):
    rig = rig_factory(
        "slack-ops",
        retry=NO_IN_PROCESS_RETRY,
        breaker=BreakerSpec(failure_threshold=1, recovery_seconds=4.0),
        queue=QueueSpec(max_receive_count=20, visibility_timeout_seconds=1),
    )
    rig.fake.faults(outage=True)
    rig.submit(task("HP-1"))

    first = rig.queue.receive(wait_seconds=3)
    assert len(first) == 1
    rig.worker.handle(first[0])
    assert rig.worker.breaker.state is BreakerState.OPEN

    rig.fake.clear_faults()
    held = rig.queue.receive(wait_seconds=3)
    assert [m.envelope.task.id for m in held] == ["HP-1"]
    assert rig.worker.handle(held[0]) is None
    assert rig.fake.inbox()["count"] == 0

    deadline = time.monotonic() + 2.5
    while time.monotonic() < deadline:
        assert rig.queue.receive(wait_seconds=1) == []

    time.sleep(rig.worker.breaker.remaining() + 0.2)
    stats = rig.worker.run(idle_polls=3, wait_seconds=2)
    assert stats.delivered == 1 and stats.deduplicated == 0
    inbox = rig.fake.inbox()
    assert inbox["count"] == 1 and inbox["unique_keys"] == 1
    assert list_dead_letters(rig.dlq) == []
