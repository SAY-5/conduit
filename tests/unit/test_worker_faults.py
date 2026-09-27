"""Failure paths around the worker's bookkeeping: they are logged, never fatal."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError
from conduit.config import ConnectorSpec, QueueSpec, RetryPolicy
from conduit.core.idempotency import MemoryIdempotencyStore
from conduit.core.queue import SqsQueue
from conduit.worker import Worker
from structlog.testing import capture_logs

from tests.unit.test_worker import FakeQueue, ScriptedAdapter, envelope


@pytest.fixture
def spec() -> ConnectorSpec:
    return ConnectorSpec(
        name="slack-ops",
        type="slack",
        target="#ops",
        secrets={"token": "SLACK_BOT_TOKEN"},
        retry=RetryPolicy(max_attempts=2, base_seconds=0.01, max_seconds=0.05),
        queue=QueueSpec(max_receive_count=2),
    )


def throttled(operation: str) -> ClientError:
    error = {"Code": "ProvisionedThroughputExceededException", "Message": "slow down"}
    return ClientError({"Error": error}, operation)


class FlakyStatusStore:
    """Refuses the first ``failures`` writes the way a throttled table would."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.published = []

    def publish(self, status) -> None:
        if self.failures:
            self.failures -= 1
            raise throttled("PutItem")
        self.published.append(status)


def test_a_worker_needs_somewhere_to_put_what_it_cannot_deliver(spec):
    with pytest.raises(TypeError):
        Worker(spec, ScriptedAdapter(spec, {}), MemoryIdempotencyStore(), FakeQueue(2))


def test_a_refused_status_write_is_logged_and_retried_not_fatal(spec):
    statuses = FlakyStatusStore(failures=1)
    queue = FakeQueue(2)
    worker = Worker(
        spec,
        ScriptedAdapter(spec, {}),
        MemoryIdempotencyStore(),
        queue,
        quarantine=FakeQueue(2),
        statuses=statuses,
        sleep=lambda _: None,
    )
    queue.send(envelope(spec, "A"))
    with capture_logs() as logs:
        stats = worker.run(idle_polls=1, wait_seconds=0)

    assert stats.delivered == 1 and len(queue.deleted) == 1
    failures = [e for e in logs if e["event"] == "status.publish_failed"]
    assert len(failures) == 1 and "slow down" in failures[0]["error"]
    assert statuses.published and statuses.published[-1].delivered == 1


class RefusingSqs:
    """Empty receives; every GetQueueAttributes call is throttled."""

    def __init__(self) -> None:
        self.attribute_calls = 0

    def receive_message(self, **_):
        return {}

    def get_queue_attributes(self, **_):
        self.attribute_calls += 1
        raise throttled("GetQueueAttributes")


def test_a_refused_depth_read_is_logged_and_the_run_goes_on(spec):
    sqs = RefusingSqs()
    queue = SqsQueue("http://sqs/000000000000/conduit-slack-ops", sqs)
    quarantine = SqsQueue("http://sqs/000000000000/conduit-slack-ops-quarantine", sqs)
    worker = Worker(
        spec, ScriptedAdapter(spec, {}), MemoryIdempotencyStore(), queue, quarantine=quarantine
    )
    with capture_logs() as logs:
        stats = worker.run(idle_polls=1, wait_seconds=0)

    assert stats.received == 0 and sqs.attribute_calls == 2
    assert [e["queue"] for e in logs if e["event"] == "queue_depth.failed"] == [
        "work",
        "quarantine",
    ]
