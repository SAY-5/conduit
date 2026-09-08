"""Retry classification and exponential backoff with full jitter."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import TypeVar

import httpx

from conduit.config import RetryPolicy

T = TypeVar("T")


class DeliveryError(Exception):
    """Base for adapter failures. ``retryable`` decides the worker's response."""

    retryable = False

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class TransientError(DeliveryError):
    """429, 5xx, timeouts, connection resets: retry with backoff."""

    retryable = True

    def __init__(
        self, message: str, *, status: int | None = None, retry_after: float | None = None
    ) -> None:
        super().__init__(message, status=status)
        self.retry_after = retry_after


class PermanentError(DeliveryError):
    """Hard 4xx (bad payload, auth, not found): do not retry, let SQS redrive."""

    retryable = False


def classify_response(response: httpx.Response, policy: RetryPolicy) -> DeliveryError | None:
    """Return the error a response represents, or None when it succeeded."""
    if response.is_success:
        return None
    status = response.status_code
    body = response.text[:200]
    if status in policy.retry_on_status or status >= 500:
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        return TransientError(f"HTTP {status}: {body}", status=status, retry_after=retry_after)
    return PermanentError(f"HTTP {status}: {body}", status=status)


def classify_exception(exc: Exception) -> DeliveryError:
    if isinstance(exc, DeliveryError):
        return exc
    if isinstance(exc, httpx.TimeoutException | httpx.NetworkError | httpx.RemoteProtocolError):
        return TransientError(f"{type(exc).__name__}: {exc}")
    return PermanentError(f"{type(exc).__name__}: {exc}")


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def backoff_delay(attempt: int, policy: RetryPolicy, rng: random.Random | None = None) -> float:
    """Full-jitter delay for ``attempt`` (1-based): uniform(0, min(cap, base * mult^(n-1)))."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    ceiling = min(policy.max_seconds, policy.base_seconds * (policy.multiplier ** (attempt - 1)))
    return (rng or random).uniform(0, ceiling)


def backoff_schedule(policy: RetryPolicy, rng: random.Random | None = None) -> Iterator[float]:
    for attempt in range(1, policy.max_attempts):
        yield backoff_delay(attempt, policy, rng)


@dataclass
class RetryOutcome:
    attempts: int
    delays: list[float] = field(default_factory=list)


def retry_call(
    fn: Callable[[], T],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, float, TransientError], None] | None = None,
) -> tuple[T, RetryOutcome]:
    """Call ``fn`` until it succeeds, raises a PermanentError, or exhausts attempts.

    Only ``TransientError`` triggers a retry. When ``max_attempts`` is exhausted the last
    ``TransientError`` is re-raised so the caller (the SQS worker) can leave the message
    for redrive instead of acknowledging it.
    """
    outcome = RetryOutcome(attempts=0)
    while True:
        outcome.attempts += 1
        try:
            return fn(), outcome
        except Exception as exc:
            err = classify_exception(exc)
            if not err.retryable or outcome.attempts >= policy.max_attempts:
                raise err from exc
            assert isinstance(err, TransientError)
            delay = backoff_delay(outcome.attempts, policy, rng)
            if err.retry_after is not None:
                delay = min(policy.max_seconds, max(delay, err.retry_after))
            outcome.delays.append(delay)
            if on_retry:
                on_retry(outcome.attempts, delay, err)
            sleep(delay)
