"""Prometheus metrics exposed by every worker on ``/metrics``."""

from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

LABELS = ["connector"]

delivered = Counter("conduit_delivered_total", "Tasks delivered to the remote system", LABELS)
deduplicated = Counter(
    "conduit_deduplicated_total", "Tasks skipped because the idempotency key was seen", LABELS
)
retried = Counter("conduit_retried_total", "Transient failures that were retried", LABELS)
failed = Counter(
    "conduit_failed_total", "Deliveries returned to the queue for redrive", LABELS + ["reason"]
)
dead_lettered = Counter(
    "conduit_dead_lettered_total", "Deliveries that exhausted maxReceiveCount", LABELS
)
latency = Histogram(
    "conduit_delivery_seconds",
    "Wall time from first attempt to acknowledged delivery",
    LABELS,
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
queue_lag = Histogram(
    "conduit_queue_lag_seconds",
    "Time between submit and the worker picking the message up",
    LABELS,
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300),
)


def serve(port: int) -> None:
    start_http_server(port)
