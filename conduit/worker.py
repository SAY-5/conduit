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
from conduit.core.idempotency import IdempotencyStore
from conduit.core.queue import Message, SqsQueue
from conduit.core.retry import DeliveryError, TransientError, retry_call
from conduit.models import DeliveryResult, DeliveryStatus

log = structlog.get_logger()


@dataclass
class WorkerStats:
    received: int = 0
    delivered: int = 0
    deduplicated: int = 0
    retried: int = 0
    failed: int = 0
    dead_lettered: int = 0
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
        self._min_interval = 1.0 / spec.rate_limit.requests_per_second
        self._last_send = 0.0
        self._log = log.bind(connector=spec.name, type=spec.type)

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
            batch = self.queue.receive(wait_seconds=wait_seconds)
            if not batch:
                idle += 1
                if idle_polls is not None and idle >= idle_polls:
                    break
                continue
            idle = 0
            for message in batch:
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
            logger.info("delivery.in_progress_elsewhere")
            return None

        remote_id = self.store.latest(name, task.id) if task.version > 1 else None
        started = self._clock()

        def on_retry(attempt: int, delay: float, err: TransientError) -> None:
            self.stats.retried += 1
            self.stats.retry_delays.append(delay)
            metrics.retried.labels(name).inc()
            budget = int(delay + self.spec.retry.timeout_seconds) + 5
            self.queue.extend_visibility(message.receipt_handle, budget)
            logger.warning("delivery.retry", attempt=attempt, delay=round(delay, 3), error=str(err))

        try:
            self._throttle()
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
        now = self._clock()
        wait = self._min_interval - (now - self._last_send)
        if wait > 0:
            self._sleep(wait)
        self._last_send = self._clock()

    def summary(self) -> dict[str, float | int]:
        s = self.stats
        return {
            "received": s.received,
            "delivered": s.delivered,
            "deduplicated": s.deduplicated,
            "retried": s.retried,
            "failed": s.failed,
            "dead_lettered": s.dead_lettered,
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
