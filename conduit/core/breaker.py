"""Per-target circuit breaker: pause a connector while its remote is failing.

Consecutive transient failures that look like the target is down (5xx, timeouts,
connection errors; not 429, which is rate limiting) open the breaker. While it is
open the worker neither polls nor delivers, so queued messages keep their receive
count and are never dead-lettered because of an outage. After ``recovery_seconds``
one message is let through as a probe: success closes the breaker, failure opens
it again for another recovery window, and a probe that ends without a verdict is
handed back so the next delivery can probe instead.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import StrEnum


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


STATE_VALUE = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_seconds: float = 30.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1 or recovery_seconds <= 0:
            raise ValueError("failure_threshold must be >= 1 and recovery_seconds > 0")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._clock = clock
        self.failures = 0
        self.opens = 0
        self._opened_at: float | None = None
        self._probing = False

    @property
    def state(self) -> BreakerState:
        if self._opened_at is None:
            return BreakerState.CLOSED
        if self._clock() - self._opened_at >= self.recovery_seconds:
            return BreakerState.HALF_OPEN
        return BreakerState.OPEN

    def remaining(self) -> float:
        """Seconds until the breaker half-opens; 0 unless it is open."""
        if self._opened_at is None:
            return 0.0
        return max(0.0, self.recovery_seconds - (self._clock() - self._opened_at))

    def allow(self) -> bool:
        """Whether a delivery may proceed now. Half-open admits exactly one probe."""
        state = self.state
        if state is BreakerState.CLOSED:
            return True
        if state is BreakerState.OPEN or self._probing:
            return False
        self._probing = True
        return True

    @property
    def probing(self) -> bool:
        """Whether a half-open probe is in flight."""
        return self._probing

    def abandon_probe(self) -> None:
        """Hand back a probe that ended without a verdict.

        A delivery that took the probe but reached neither ``record_success``
        nor ``record_failure`` (the message was a duplicate, another worker held
        the claim, or the retries exhausted on 429) has said nothing about the
        target, so the next delivery must be able to probe.
        """
        self._probing = False

    def record_success(self) -> bool:
        """Reset; returns True when this closed a half-open breaker."""
        was_open = self._opened_at is not None
        self.failures = 0
        self._opened_at = None
        self._probing = False
        return was_open

    def record_failure(self) -> bool:
        """Count one target failure; returns True when this opened the breaker."""
        self.failures += 1
        tripped = self._probing or self.failures >= self.failure_threshold
        if not tripped:
            return False
        self._opened_at = self._clock()
        self._probing = False
        self.failures = 0
        self.opens += 1
        return True
