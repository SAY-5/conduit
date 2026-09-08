"""Worker semantics with an in-memory queue: dedup, retry, redrive, dead-letter accounting."""

from __future__ import annotations

from collections import deque

import pytest
from conduit import metrics
from conduit.adapters.base import Adapter
from conduit.config import ConnectorSpec, FieldRule, QueueSpec, RetryPolicy
from conduit.core.idempotency import MemoryIdempotencyStore, idempotency_key
from conduit.core.queue import Message
from conduit.core.retry import PermanentError, TransientError
from conduit.models import DeliveryResult, DeliveryStatus, Envelope, Task
from conduit.worker import Worker, percentile


class FakeQueue:
    """Mimics SQS receive/delete/visibility and the redrive to a DLQ."""

    def __init__(self, max_receive_count: int) -> None:
        self.name = "fake"
        self.pending: deque[tuple[Envelope, int]] = deque()
        self.deleted: list[str] = []
        self.dlq: list[Envelope] = []
        self.visibility_calls: list[int] = []
        self.max_receive_count = max_receive_count
        self._receipts: dict[str, tuple[Envelope, int]] = {}

    def send(self, envelope: Envelope) -> None:
        self.pending.append((envelope, 0))

    def receive(self, **_) -> list[Message]:
        if not self.pending:
            return []
        envelope, count = self.pending.popleft()
        count += 1
        if count > self.max_receive_count:
            self.dlq.append(envelope)
            return self.receive()
        receipt = f"r-{envelope.idempotency_key[:8]}-{count}"
        self._receipts[receipt] = (envelope, count)
        return [Message(receipt, "m", envelope, count)]

    def delete(self, receipt: str) -> None:
        self.deleted.append(receipt)
        self._receipts.pop(receipt, None)

    def extend_visibility(self, receipt: str, seconds: int) -> None:
        self.visibility_calls.append(seconds)
        if seconds == 0 and receipt in self._receipts:
            self.pending.append(self._receipts.pop(receipt))


class ScriptedAdapter(Adapter):
    type = "slack"

    def __init__(self, spec: ConnectorSpec, script: dict[str, list]) -> None:
        self.spec = spec
        self.script = script
        self.calls: list[tuple[str, int, str | None]] = []

    def deliver(self, task, idempotency_key, remote_id=None):
        self.calls.append((task.id, task.version, remote_id))
        outcomes = self.script.get(task.id)
        if outcomes:
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
        return DeliveryResult(
            status=DeliveryStatus.DELIVERED,
            connector=self.spec.name,
            task_id=task.id,
            idempotency_key=idempotency_key,
            remote_id=f"remote-{task.id}",
        )

    def healthcheck(self) -> bool:
        return True

    def close(self) -> None:
        pass


@pytest.fixture
def spec() -> ConnectorSpec:
    return ConnectorSpec(
        name="slack-ops",
        type="slack",
        target="#ops",
        secrets={"token": "SLACK_BOT_TOKEN"},
        retry=RetryPolicy(max_attempts=3, base_seconds=0.01, max_seconds=0.05),
        queue=QueueSpec(max_receive_count=2),
    )


def envelope(spec: ConnectorSpec, task_id: str, version: int = 1) -> Envelope:
    return Envelope(
        task=Task(id=task_id, version=version, title=f"task {task_id}"),
        connector=spec.name,
        idempotency_key=idempotency_key(spec.name, task_id, version),
    )


def make_worker(spec, script):
    queue = FakeQueue(spec.queue.max_receive_count)
    adapter = ScriptedAdapter(spec, script)
    worker = Worker(spec, adapter, MemoryIdempotencyStore(), queue, sleep=lambda _: None)
    return worker, queue, adapter


def test_delivers_and_acknowledges(spec):
    worker, queue, adapter = make_worker(spec, {})
    queue.send(envelope(spec, "A"))
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and len(queue.deleted) == 1
    assert worker.summary()["delivered"] == 1
    assert adapter.calls == [("A", 1, None)]


def test_duplicate_submit_is_deduplicated_not_redelivered(spec):
    worker, queue, adapter = make_worker(spec, {})
    queue.send(envelope(spec, "A"))
    queue.send(envelope(spec, "A"))
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and stats.deduplicated == 1
    assert len(adapter.calls) == 1
    assert len(queue.deleted) == 2


def test_new_version_updates_using_recorded_remote_id(spec):
    worker, queue, adapter = make_worker(spec, {})
    queue.send(envelope(spec, "A", 1))
    queue.send(envelope(spec, "A", 2))
    worker.run(idle_polls=1, wait_seconds=0)
    assert adapter.calls == [("A", 1, None), ("A", 2, "remote-A")]


def test_transient_failure_is_retried_in_process(spec):
    script = {"A": [TransientError("429", status=429), TransientError("503", status=503)]}
    worker, queue, adapter = make_worker(spec, script)
    queue.send(envelope(spec, "A"))
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and stats.retried == 2
    assert len(adapter.calls) == 3
    assert len(stats.retry_delays) == 2 and all(0 <= d <= 0.05 for d in stats.retry_delays)
    assert queue.visibility_calls and all(v > 0 for v in queue.visibility_calls)


def test_exhausted_retries_return_message_for_redrive(spec):
    script = {"A": [TransientError("503", status=503)] * 3}
    worker, queue, adapter = make_worker(spec, script)
    queue.send(envelope(spec, "A"))
    result = worker.handle(queue.receive()[0])
    assert result.status == DeliveryStatus.FAILED and "503" in result.detail
    assert queue.deleted == [] and queue.pending
    assert worker.stats.failed == 1 and worker.stats.dead_lettered == 0
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and len(adapter.calls) == 4


def test_permanent_failure_reaches_dlq_after_max_receive_count(spec):
    script = {"A": [PermanentError("400", status=400)] * 5}
    worker, queue, adapter = make_worker(spec, script)
    queue.send(envelope(spec, "A"))
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 0 and stats.failed == 2 and stats.retried == 0
    assert stats.dead_lettered == 1
    assert len(adapter.calls) == 2 == spec.queue.max_receive_count
    assert [e.task.id for e in queue.dlq] == ["A"]
    assert queue.deleted == []


def test_replayed_dead_letter_delivers_after_fix(spec):
    script = {"A": [PermanentError("400", status=400)] * 2}
    worker, queue, adapter = make_worker(spec, script)
    queue.send(envelope(spec, "A"))
    worker.run(idle_polls=1, wait_seconds=0)
    assert queue.dlq
    replayed = queue.dlq.pop().model_copy(update={"attempt": 1})
    queue.send(replayed)
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and len(adapter.calls) == 3


def test_invalid_task_is_rejected_before_claim_and_acknowledged(spec):
    rules = {"owner": FieldRule(source="assignee", required=True)}
    spec = spec.model_copy(update={"mapping": rules})
    worker, queue, adapter = make_worker(spec, {})
    before = metrics.rejected.labels(spec.name, "required")._value.get()
    queue.send(envelope(spec, "A"))
    result = worker.handle(queue.receive()[0])
    assert result.status == DeliveryStatus.REJECTED and "owner" in result.detail
    assert adapter.calls == [] and len(queue.deleted) == 1 and queue.dlq == []
    assert worker.stats.rejected == 1 and worker.summary()["rejected"] == 1
    assert metrics.rejected.labels(spec.name, "required")._value.get() == before + 1
    assert worker.store.claim(
        envelope(spec, "A").idempotency_key, connector=spec.name, task_id="A"
    ).acquired


def test_in_progress_elsewhere_is_left_alone(spec):
    worker, queue, adapter = make_worker(spec, {})
    env = envelope(spec, "A")
    worker.store.claim(env.idempotency_key, connector=spec.name, task_id="A")
    queue.send(env)
    assert worker.handle(queue.receive()[0]) is None
    assert adapter.calls == [] and queue.deleted == []


def test_rate_limit_spacing(spec):
    spec = spec.model_copy(
        update={"rate_limit": spec.rate_limit.model_copy(update={"requests_per_second": 2})}
    )
    clock = {"t": 0.0}
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    queue = FakeQueue(2)
    worker = Worker(
        spec,
        ScriptedAdapter(spec, {}),
        MemoryIdempotencyStore(),
        queue,
        sleep=sleep,
        clock=lambda: clock["t"],
    )
    for i in range(3):
        queue.send(envelope(spec, f"T{i}"))
    worker.run(idle_polls=1, wait_seconds=0)
    assert len(slept) == 2 and all(abs(s - 0.5) < 1e-9 for s in slept)


def test_percentile():
    assert percentile([], 50) == 0
    assert percentile([1, 2, 3, 4, 5], 50) == 3
    assert percentile([1, 2, 3, 4, 5], 95) == pytest.approx(4.8)
