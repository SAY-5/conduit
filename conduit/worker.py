"""Per-connector worker: SQS in, adapter out, idempotency and retry in between."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog

from conduit import metrics
from conduit.adapters.base import Adapter
from conduit.config import ConnectorSpec
from conduit.core.breaker import STATE_VALUE, BreakerState, CircuitBreaker
from conduit.core.idempotency import IdempotencyStore
from conduit.core.mapping import validate_task
from conduit.core.queue import Message, SqsQueue
from conduit.core.ratelimit import TokenBucket
from conduit.core.retry import DeliveryError, TransientError, retry_call
from conduit.models import DeliveryResult, DeliveryStatus

log = structlog.get_logger()

# A duplicate that arrives while another worker still holds the claim is re-polled
# after this many seconds instead of the queue's full visibility timeout.
IN_PROGRESS_RECHECK_SECONDS = 10
# While the breaker is open the worker sleeps in slices this long so a stop
# request and the visibility heartbeat on held messages both stay responsive.
BREAKER_PAUSE_SLICE = 1.0


def is_target_failure(err: TransientError) -> bool:
    """5xx, timeouts, and connection errors mean the target is unwell; 429 is throttling."""
    return err.status != 429


@dataclass
class WorkerStats:
    received: int = 0
    delivered: int = 0
    deduplicated: int = 0
    rejected: int = 0
    retried: int = 0
    failed: int = 0
    dead_lettered: int = 0
    rate_limit_waits: int = 0
    rate_limit_wait_seconds: float = 0.0
    retry_after_honored: int = 0
    breaker_opens: int = 0
    breaker_paused_seconds: float = 0.0
    latencies: list[float] = field(default_factory=list)
    retry_delays: list[float] = field(default_factory=list)


class Worker:
    def __init__(
        self,
        spec: ConnectorSpec,
        adapter: Adapter,
        store: IdempotencyStore,
        queue: SqsQueue,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.spec = spec
        self.adapter = adapter
        self.store = store
        self.queue = queue
        self._sleep = sleep
        self._clock = clock
        self.stats = WorkerStats()
        self.bucket = TokenBucket(
            spec.rate_limit.requests_per_second,
            spec.rate_limit.burst,
            max_penalty=spec.rate_limit.max_retry_after_seconds,
            clock=clock,
        )
        self.breaker = CircuitBreaker(
            spec.breaker.failure_threshold, spec.breaker.recovery_seconds, clock=clock
        )
        self._log = log.bind(connector=spec.name, type=spec.type)
        metrics.breaker_state.labels(spec.name).set(0)

    def run(
        self,
        *,
        stop: threading.Event | None = None,
        max_messages: int | None = None,
        idle_polls: int | None = None,
        wait_seconds: int = 20,
    ) -> WorkerStats:
        """Poll until ``stop`` is set, ``max_messages`` handled, or ``idle_polls`` empty polls."""
        stop = stop or threading.Event()
        handled = 0
        idle = 0
        self._log.info("worker.start", queue=self.queue.name)
        while not stop.is_set():
            if self.breaker.state is BreakerState.OPEN:
                self._pause_while_open([], stop)
                continue
            batch = self.queue.receive(wait_seconds=wait_seconds)
            if getattr(batch, "received_count", len(batch)) == 0:
                idle += 1
                if idle_polls is not None and idle >= idle_polls:
                    break
                continue
            idle = 0
            for index, message in enumerate(batch):
                if self.breaker.state is BreakerState.OPEN:
                    self._pause_while_open(batch[index:], stop)
                    if stop.is_set():
                        break
                self.handle(message)
                handled += 1
                if max_messages is not None and handled >= max_messages:
                    stop.set()
                    break
        self._log.info("worker.stop", **self.summary())
        return self.stats

    def handle(self, message: Message) -> DeliveryResult | None:
        env = message.envelope
        task = env.task
        key = env.idempotency_key
        name = self.spec.name
        self.stats.received += 1
        lag = (datetime.now(UTC) - env.submitted_at).total_seconds()
        metrics.queue_lag.labels(name).observe(max(0.0, lag))
        logger = self._log.bind(task_id=task.id, version=task.version, key=key[:12])

        problem = validate_task(self.spec, task)
        if problem is not None:
            self.queue.delete(message.receipt_handle)
            self.stats.rejected += 1
            metrics.rejected.labels(name, problem.reason).inc()
            logger.warning(
                "delivery.rejected", field=problem.field, reason=problem.reason, error=str(problem)
            )
            return DeliveryResult(
                status=DeliveryStatus.REJECTED,
                connector=name,
                task_id=task.id,
                idempotency_key=key,
                detail=f"mapping {problem.reason}: {problem}",
            )

        if not self.breaker.allow():
            hold = int(self.breaker.remaining()) + 1
            self.queue.extend_visibility(message.receipt_handle, hold)
            logger.info("delivery.breaker_open", hold=hold)
            return None

        claim = self.store.claim(key, connector=name, task_id=task.id)
        if not claim.acquired:
            if claim.duplicate:
                self.queue.delete(message.receipt_handle)
                self.stats.deduplicated += 1
                metrics.deduplicated.labels(name).inc()
                logger.info("delivery.deduplicated", remote_id=claim.remote_id)
                return DeliveryResult(
                    status=DeliveryStatus.DEDUPLICATED,
                    connector=name,
                    task_id=task.id,
                    idempotency_key=key,
                    remote_id=claim.remote_id,
                    detail="idempotency key already delivered",
                )
            self.queue.extend_visibility(message.receipt_handle, IN_PROGRESS_RECHECK_SECONDS)
            logger.info("delivery.in_progress_elsewhere", recheck=IN_PROGRESS_RECHECK_SECONDS)
            return None

        remote_id = self.store.latest(name, task.id) if task.version > 1 else None
        self._throttle()
        started = self._clock()

        def on_retry(attempt: int, delay: float, err: TransientError) -> None:
            self.stats.retried += 1
            self.stats.retry_delays.append(delay)
            metrics.retried.labels(name).inc()
            budget = int(delay + self.spec.retry.timeout_seconds) + 5
            self.queue.extend_visibility(message.receipt_handle, budget)
            logger.warning("delivery.retry", attempt=attempt, delay=round(delay, 3), error=str(err))
            self._note_transient(err, logger)

        try:
            result, outcome = retry_call(
                lambda: self.adapter.deliver(task, key, remote_id=remote_id),
                self.spec.retry,
                sleep=self._sleep,
                on_retry=on_retry,
            )
        except DeliveryError as err:
            self.store.release(key)
            elapsed = self._clock() - started
            reason = "permanent" if not err.retryable else "exhausted"
            if isinstance(err, TransientError):
                self._note_transient(err, logger)
            else:
                self._note_target_ok(logger)
            self.stats.failed += 1
            metrics.failed.labels(name, reason).inc()
            will_dead_letter = message.receive_count >= self.spec.queue.max_receive_count
            if will_dead_letter:
                self.stats.dead_lettered += 1
                metrics.dead_lettered.labels(name).inc()
            logger.error(
                "delivery.failed",
                reason=reason,
                error=str(err),
                receive_count=message.receive_count,
                elapsed=round(elapsed, 3),
                dead_letter=will_dead_letter,
            )
            self.queue.extend_visibility(message.receipt_handle, 0)
            return DeliveryResult(
                status=DeliveryStatus.FAILED,
                connector=name,
                task_id=task.id,
                idempotency_key=key,
                attempts=message.receive_count,
                detail=str(err),
            )

        elapsed = self._clock() - started
        self._note_target_ok(logger)
        self.store.mark_delivered(key, result.remote_id)
        self.queue.delete(message.receipt_handle)
        self.stats.delivered += 1
        self.stats.latencies.append(elapsed)
        metrics.delivered.labels(name).inc()
        metrics.latency.labels(name).observe(elapsed)
        logger.info(
            "delivery.ok",
            remote_id=result.remote_id,
            attempts=outcome.attempts,
            elapsed=round(elapsed, 3),
        )
        return result.model_copy(update={"attempts": outcome.attempts})

    def _throttle(self) -> None:
        wait = self.bucket.acquire()
        if wait <= 0:
            return
        self.stats.rate_limit_waits += 1
        self.stats.rate_limit_wait_seconds += wait
        metrics.rate_limit_waits.labels(self.spec.name).inc()
        metrics.rate_limit_wait_seconds.labels(self.spec.name).inc(wait)
        self._sleep(wait)

    def _note_transient(self, err: TransientError, logger) -> None:
        """Feed one transient attempt to the bucket (429) or the breaker (target down)."""
        name = self.spec.name
        if err.status == 429 and err.retry_after is not None:
            pause = self.bucket.penalize(err.retry_after)
            self.stats.retry_after_honored += 1
            metrics.retry_after_honored.labels(name).inc()
            logger.info("rate_limit.retry_after", pause=round(pause, 3))
        if is_target_failure(err) and self.breaker.record_failure():
            self.stats.breaker_opens += 1
            metrics.breaker_opens.labels(name).inc()
            metrics.breaker_state.labels(name).set(STATE_VALUE[BreakerState.OPEN])
            logger.error(
                "breaker.open",
                threshold=self.breaker.failure_threshold,
                recovery=self.breaker.recovery_seconds,
            )

    def _note_target_ok(self, logger) -> None:
        if self.breaker.record_success():
            metrics.breaker_state.labels(self.spec.name).set(STATE_VALUE[BreakerState.CLOSED])
            logger.info("breaker.closed")

    def _pause_while_open(self, held: list[Message], stop: threading.Event) -> None:
        """Sleep until the breaker half-opens, keeping any held messages invisible."""
        name = self.spec.name
        metrics.breaker_state.labels(name).set(STATE_VALUE[BreakerState.OPEN])
        self._log.warning("breaker.paused", remaining=round(self.breaker.remaining(), 3))
        while self.breaker.state is BreakerState.OPEN and not stop.is_set():
            remaining = self.breaker.remaining()
            for message in held:
                self.queue.extend_visibility(message.receipt_handle, int(remaining) + 5)
            nap = min(BREAKER_PAUSE_SLICE, remaining)
            self._sleep(nap)
            self.stats.breaker_paused_seconds += nap
            metrics.breaker_paused_seconds.labels(name).inc(nap)
        if self.breaker.state is BreakerState.HALF_OPEN:
            metrics.breaker_state.labels(name).set(STATE_VALUE[BreakerState.HALF_OPEN])
            self._log.info("breaker.half_open")

    def summary(self) -> dict[str, float | int | str]:
        s = self.stats
        return {
            "received": s.received,
            "delivered": s.delivered,
            "deduplicated": s.deduplicated,
            "rejected": s.rejected,
            "retried": s.retried,
            "failed": s.failed,
            "dead_lettered": s.dead_lettered,
            "rate_limit_waits": s.rate_limit_waits,
            "retry_after_honored": s.retry_after_honored,
            "breaker_state": str(self.breaker.state),
            "breaker_opens": s.breaker_opens,
            "breaker_paused_seconds": round(s.breaker_paused_seconds, 3),
            "p50_ms": round(percentile(s.latencies, 50) * 1000, 1),
            "p95_ms": round(percentile(s.latencies, 95) * 1000, 1),
        }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = (pct / 100) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)
