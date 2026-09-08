"""Token-bucket rate limit per connector that also honours ``Retry-After``."""

from __future__ import annotations

import time
from collections.abc import Callable


class TokenBucket:
    """``rate`` tokens per second up to ``burst``; ``acquire`` reserves one token.

    The bucket never blocks. ``acquire`` returns how long the caller must wait
    before the reserved token is valid, so the worker sleeps with its own
    injectable clock. Tokens may go negative while reservations are outstanding,
    which is what keeps consecutive sends ``1 / rate`` apart once the burst is
    spent. ``penalize`` moves the earliest allowed send into the future when the
    remote side answers 429 with ``Retry-After``; the pause is shared by every
    message on the connector, not only the one that was rate limited.
    """

    def __init__(
        self,
        rate: float,
        burst: int = 1,
        *,
        max_penalty: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0 or burst < 1:
            raise ValueError("rate must be positive and burst at least 1")
        self.rate = rate
        self.capacity = float(burst)
        self.max_penalty = max_penalty
        self._clock = clock
        self.tokens = float(burst)
        self._updated = clock()
        self.not_before = self._updated

    def _refill(self) -> float:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self._updated = now
        return now

    def acquire(self) -> float:
        """Reserve one token; return the seconds to wait before using it (0 when free)."""
        now = self._refill()
        wait = 0.0 if self.tokens >= 1.0 else (1.0 - self.tokens) / self.rate
        self.tokens -= 1.0
        return max(wait, self.not_before - now, 0.0)

    def penalize(self, seconds: float) -> float:
        """Pause every send for ``seconds`` (capped at ``max_penalty``); return the pause."""
        pause = min(max(0.0, seconds), self.max_penalty)
        self.not_before = max(self.not_before, self._clock() + pause)
        return pause

    def available(self) -> float:
        self._refill()
        return max(0.0, self.tokens)
