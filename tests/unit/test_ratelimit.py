"""Token bucket: burst, steady rate over a window, and Retry-After penalties."""

from __future__ import annotations

import pytest
from conduit.core.ratelimit import TokenBucket


class Clock:
    """Monotonic clock the test advances by the waits the bucket asks for."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def send(bucket: TokenBucket, clock: Clock) -> float:
    """Do what the worker does: acquire, sleep the wait, return the send time."""
    wait = bucket.acquire()
    clock.advance(wait)
    return clock.now


@pytest.mark.parametrize("rate,burst", [(5.0, 1), (10.0, 3), (2.0, 5)])
def test_bucket_holds_the_configured_rate_over_a_window(rate: float, burst: int):
    clock = Clock()
    bucket = TokenBucket(rate, burst, clock=clock)
    sends = [send(bucket, clock) for _ in range(int(rate) * 4)]
    window = sends[-1] - sends[0]
    assert len(sends) - burst == pytest.approx(window * rate)
    assert window > 0


def test_burst_is_free_then_sends_are_spaced():
    clock = Clock()
    bucket = TokenBucket(4.0, 3, clock=clock)
    assert [bucket.acquire() for _ in range(3)] == [0.0, 0.0, 0.0]
    assert bucket.acquire() == pytest.approx(0.25)


def test_tokens_refill_while_idle():
    clock = Clock()
    bucket = TokenBucket(10.0, 5, clock=clock)
    for _ in range(5):
        bucket.acquire()
    assert bucket.available() == 0
    clock.advance(0.3)
    assert bucket.available() == pytest.approx(3.0)
    clock.advance(10)
    assert bucket.available() == 5.0


def test_retry_after_pauses_every_send_on_the_connector():
    clock = Clock()
    bucket = TokenBucket(100.0, 10, clock=clock)
    assert bucket.penalize(2.0) == 2.0
    assert bucket.acquire() == pytest.approx(2.0)
    clock.advance(2.0)
    assert bucket.acquire() == 0.0


def test_retry_after_is_capped():
    clock = Clock()
    bucket = TokenBucket(1.0, 1, max_penalty=30.0, clock=clock)
    assert bucket.penalize(3600) == 30.0
    assert bucket.acquire() == pytest.approx(30.0)


def test_a_shorter_penalty_never_shortens_a_longer_one():
    clock = Clock()
    bucket = TokenBucket(100.0, 10, clock=clock)
    bucket.penalize(5.0)
    bucket.penalize(1.0)
    assert bucket.acquire() == pytest.approx(5.0)


def test_rate_and_burst_are_validated():
    with pytest.raises(ValueError):
        TokenBucket(0, 1)
    with pytest.raises(ValueError):
        TokenBucket(1, 0)
