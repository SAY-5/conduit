"""Worker semantics with an in-memory queue: dedup, retry, redrive, dead-letter accounting."""

from __future__ import annotations

import time
from collections import deque

import pytest
from conduit import metrics
from conduit.adapters.base import Adapter
from conduit.config import BreakerSpec, ConnectorSpec, FieldRule, QueueSpec, RetryPolicy
from conduit.core.breaker import BreakerState
from conduit.core.idempotency import MemoryIdempotencyStore, idempotency_key
from conduit.core.queue import Message, SqsQueue
from conduit.core.retry import PermanentError, TransientError
from conduit.core.schema import SchemaField, SourceSchema
from conduit.models import DeliveryResult, DeliveryStatus, Envelope, Task
from conduit.worker import QUEUE_DEPTH_INTERVAL_SECONDS, Worker, percentile
from prometheus_client import REGISTRY


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


def make_worker(spec, script, *, quarantine=None, schema=None, clock=time.monotonic):
    queue = FakeQueue(spec.queue.max_receive_count)
    adapter = ScriptedAdapter(spec, script)
    worker = Worker(
        spec,
        adapter,
        MemoryIdempotencyStore(),
        queue,
        quarantine=quarantine or FakeQueue(spec.queue.max_receive_count),
        schema=schema,
        sleep=lambda _: None,
        clock=clock,
    )
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


def test_invalid_task_is_quarantined_before_claim_and_acknowledged(spec):
    rules = {"owner": FieldRule(source="assignee", required=True)}
    spec = spec.model_copy(update={"mapping": rules})
    quarantine = FakeQueue(spec.queue.max_receive_count)
    worker, queue, adapter = make_worker(spec, {}, quarantine=quarantine)
    before = metrics.quarantined.labels(spec.name, "mapping", "required")._value.get()
    queue.send(envelope(spec, "A"))
    result = worker.handle(queue.receive()[0])
    assert result.status == DeliveryStatus.QUARANTINED and "owner" in result.detail
    assert adapter.calls == [] and len(queue.deleted) == 1 and queue.dlq == []
    assert worker.stats.quarantined == 1 and worker.summary()["quarantined"] == 1
    assert metrics.quarantined.labels(spec.name, "mapping", "required")._value.get() == before + 1
    assert [env.task.id for env, _ in quarantine.pending] == ["A"]
    assert worker.store.claim(
        envelope(spec, "A").idempotency_key, connector=spec.name, task_id="A"
    ).acquired


def test_schema_failure_is_quarantined_before_the_mapping_runs(spec):
    schema = SourceSchema(
        connector=spec.name,
        version=1,
        fields={"fields.region": SchemaField(type="string", required=True)},
    )
    quarantine = FakeQueue(spec.queue.max_receive_count)
    worker, queue, adapter = make_worker(spec, {}, quarantine=quarantine, schema=schema)
    queue.send(envelope(spec, "A"))
    result = worker.handle(queue.receive()[0])
    assert result.status == DeliveryStatus.QUARANTINED
    assert "schema missing" in result.detail and "fields.region" in result.detail
    assert adapter.calls == [] and queue.dlq == []
    note = next(env.quarantine for env, _ in quarantine.pending)
    assert note.stage == "schema" and note.reason == "missing"


def test_a_task_that_matches_the_schema_is_delivered(spec):
    schema = SourceSchema(
        connector=spec.name,
        version=1,
        fields={"title": SchemaField(type="string", required=True)},
    )
    quarantine = FakeQueue(spec.queue.max_receive_count)
    worker, queue, adapter = make_worker(spec, {}, quarantine=quarantine, schema=schema)
    queue.send(envelope(spec, "A"))
    stats = worker.run(idle_polls=1, wait_seconds=0)
    assert stats.delivered == 1 and stats.quarantined == 0
    assert list(quarantine.pending) == []


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
        quarantine=FakeQueue(2),
        sleep=sleep,
        clock=lambda: clock["t"],
    )
    for i in range(3):
        queue.send(envelope(spec, f"T{i}"))
    worker.run(idle_polls=1, wait_seconds=0)
    assert len(slept) == 2 and all(abs(s - 0.5) < 1e-9 for s in slept)


def breaker_rig(spec, script, *, visibility_timeout_seconds=60):
    """A worker whose breaker opens on one 503 and half-opens after 5 s on the test's clock."""
    spec = spec.model_copy(
        update={
            "retry": RetryPolicy(max_attempts=1, base_seconds=0.01, max_seconds=0.05),
            "breaker": BreakerSpec(failure_threshold=1, recovery_seconds=5.0),
            "queue": QueueSpec(
                max_receive_count=10, visibility_timeout_seconds=visibility_timeout_seconds
            ),
        }
    )
    clock = {"t": 100.0}
    worker, queue, adapter = make_worker(spec, script, clock=lambda: clock["t"])
    return worker, queue, adapter, clock


def open_then_half_open(worker, message, clock):
    """Fail ``message`` against the target, then let the recovery window pass."""
    assert worker.handle(message).status == DeliveryStatus.FAILED
    assert worker.breaker.state is BreakerState.OPEN
    clock["t"] += worker.breaker.recovery_seconds
    assert worker.breaker.state is BreakerState.HALF_OPEN


def test_a_duplicate_does_not_spend_the_half_open_probe(spec):
    script = {"DOWN": [TransientError("503", status=503)]}
    worker, queue, adapter, clock = breaker_rig(spec, script)
    queue.send(envelope(spec, "OK"))
    assert worker.handle(queue.receive()[0]).status == DeliveryStatus.DELIVERED
    queue.send(envelope(spec, "DOWN"))
    down = queue.receive()[0]
    queue.send(envelope(spec, "OK"))
    open_then_half_open(worker, down, clock)

    duplicate = worker.handle(queue.receive()[0])
    assert duplicate.status == DeliveryStatus.DEDUPLICATED
    assert worker.breaker.state is BreakerState.HALF_OPEN and not worker.breaker.probing

    assert worker.handle(queue.receive()[0]).status == DeliveryStatus.DELIVERED
    assert worker.breaker.state is BreakerState.CLOSED
    assert worker.stats.delivered == 2 and worker.stats.deduplicated == 1
    assert adapter.calls == [("OK", 1, None), ("DOWN", 1, None), ("DOWN", 1, None)]


def test_a_claim_held_elsewhere_does_not_spend_the_half_open_probe(spec):
    script = {"DOWN": [TransientError("503", status=503)]}
    worker, queue, adapter, clock = breaker_rig(spec, script)
    queue.send(envelope(spec, "DOWN"))
    open_then_half_open(worker, queue.receive()[0], clock)
    down = queue.receive()[0]
    held = envelope(spec, "HELD")
    worker.store.claim(held.idempotency_key, connector=spec.name, task_id="HELD")
    queue.send(held)

    assert worker.handle(queue.receive()[0]) is None
    assert worker.breaker.state is BreakerState.HALF_OPEN and not worker.breaker.probing
    assert worker.handle(down).status == DeliveryStatus.DELIVERED
    assert worker.breaker.state is BreakerState.CLOSED


def test_a_probe_that_exhausts_on_429_is_handed_back(spec):
    script = {"DOWN": [TransientError("503", status=503), TransientError("429", status=429)]}
    worker, queue, adapter, clock = breaker_rig(spec, script)
    queue.send(envelope(spec, "DOWN"))
    open_then_half_open(worker, queue.receive()[0], clock)

    probe = worker.handle(queue.receive()[0])
    assert probe.status == DeliveryStatus.FAILED and "429" in probe.detail
    assert worker.breaker.state is BreakerState.HALF_OPEN and not worker.breaker.probing
    assert worker.stats.breaker_opens == 1

    assert worker.handle(queue.receive()[0]).status == DeliveryStatus.DELIVERED
    assert worker.breaker.state is BreakerState.CLOSED
    assert len(adapter.calls) == 3


def test_a_message_turned_away_by_an_open_breaker_waits_the_visibility_timeout(spec):
    script = {"DOWN": [TransientError("503", status=503)]}
    worker, queue, adapter, clock = breaker_rig(spec, script, visibility_timeout_seconds=45)
    queue.send(envelope(spec, "DOWN"))
    assert worker.handle(queue.receive()[0]).status == DeliveryStatus.FAILED
    assert worker.breaker.state is BreakerState.OPEN

    assert worker.handle(queue.receive()[0]) is None
    assert queue.visibility_calls[-1] == 45
    assert len(adapter.calls) == 1
    key = envelope(spec, "DOWN").idempotency_key
    assert worker.store.claim(key, connector=spec.name, task_id="DOWN").acquired


class StubSqs:
    """Empty receives, and depths per queue name that a test can change between polls."""

    def __init__(self, depths: dict[str, tuple[int, int]]) -> None:
        self.depths = depths
        self.attribute_calls = 0

    def receive_message(self, **_):
        return {}

    def get_queue_attributes(self, *, QueueUrl, AttributeNames):  # noqa: N803 - boto3 spelling
        self.attribute_calls += 1
        visible, in_flight = self.depths[QueueUrl.rsplit("/", 1)[-1]]
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(visible),
                "ApproximateNumberOfMessagesNotVisible": str(in_flight),
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }


def depth_gauge(connector: str, queue: str, visibility: str) -> float | None:
    labels = {"connector": connector, "queue": queue, "visibility": visibility}
    return REGISTRY.get_sample_value("conduit_queue_depth", labels)


def test_idle_worker_refreshes_queue_depth_at_most_once_per_interval(spec):
    spec = spec.model_copy(update={"name": "depth-probe"})
    names = (spec.queue_name, spec.dlq_name, spec.quarantine_name)
    sqs = StubSqs(dict(zip(names, [(4, 1), (2, 0), (1, 0)], strict=True)))
    queue, dlq, quarantine = (SqsQueue(f"http://sqs/000000000000/{n}", sqs) for n in names)
    clock = {"t": 100.0}
    worker = Worker(
        spec,
        ScriptedAdapter(spec, {}),
        MemoryIdempotencyStore(),
        queue,
        dlq=dlq,
        quarantine=quarantine,
        clock=lambda: clock["t"],
    )

    worker.run(idle_polls=1, wait_seconds=0)
    assert sqs.attribute_calls == 3
    assert depth_gauge(spec.name, "work", "visible") == 4
    assert depth_gauge(spec.name, "work", "in_flight") == 1
    assert depth_gauge(spec.name, "dlq", "visible") == 2
    assert depth_gauge(spec.name, "quarantine", "visible") == 1

    sqs.depths[spec.dlq_name] = (5, 0)
    clock["t"] += QUEUE_DEPTH_INTERVAL_SECONDS - 1
    worker.run(idle_polls=3, wait_seconds=0)
    assert sqs.attribute_calls == 3
    assert depth_gauge(spec.name, "dlq", "visible") == 2

    clock["t"] += 1
    worker.run(idle_polls=1, wait_seconds=0)
    assert sqs.attribute_calls == 6
    assert depth_gauge(spec.name, "dlq", "visible") == 5


def test_percentile():
    assert percentile([], 50) == 0
    assert percentile([1, 2, 3, 4, 5], 50) == 3
    assert percentile([1, 2, 3, 4, 5], 95) == pytest.approx(4.8)
