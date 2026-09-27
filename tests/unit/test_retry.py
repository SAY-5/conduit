import random

import httpx
import pytest
from conduit.config import RetryPolicy
from conduit.core.retry import (
    PermanentError,
    TransientError,
    backoff_delay,
    classify_exception,
    classify_response,
    retry_call,
)


def _resp(status: int, headers: dict | None = None) -> httpx.Response:
    return httpx.Response(status, headers=headers, request=httpx.Request("POST", "http://x"))


@pytest.mark.parametrize("status", [200, 201, 204])
def test_success_is_not_an_error(status):
    assert classify_response(_resp(status), RetryPolicy()) is None


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 408])
def test_transient_statuses(status):
    err = classify_response(_resp(status), RetryPolicy())
    assert isinstance(err, TransientError) and err.retryable and err.status == status


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_permanent_statuses(status):
    err = classify_response(_resp(status), RetryPolicy())
    assert isinstance(err, PermanentError) and not err.retryable


def test_retry_after_header_is_parsed():
    err = classify_response(_resp(429, {"Retry-After": "2"}), RetryPolicy())
    assert isinstance(err, TransientError) and err.retry_after == 2.0
    err = classify_response(_resp(429, {"Retry-After": "soon"}), RetryPolicy())
    assert err.retry_after is None


def test_exception_classification():
    assert classify_exception(httpx.ReadTimeout("t")).retryable
    assert classify_exception(httpx.ConnectError("c")).retryable
    assert not classify_exception(ValueError("bad json")).retryable
    original = PermanentError("keep")
    assert classify_exception(original) is original


def test_backoff_is_bounded_full_jitter():
    policy = RetryPolicy(base_seconds=0.5, multiplier=2, max_seconds=4, max_attempts=8)
    rng = random.Random(7)
    for attempt in range(1, 9):
        ceiling = min(4, 0.5 * 2 ** (attempt - 1))
        for _ in range(50):
            d = backoff_delay(attempt, policy, rng)
            assert 0 <= d <= ceiling
    with pytest.raises(ValueError):
        backoff_delay(0, policy)


def test_backoff_ceiling_grows_then_caps():
    policy = RetryPolicy(base_seconds=1, multiplier=2, max_seconds=5, max_attempts=6)

    class Max(random.Random):
        def uniform(self, a, b):
            return b

    delays = [backoff_delay(n, policy, Max()) for n in range(1, 6)]
    assert delays == [1, 2, 4, 5, 5]


def test_retry_call_retries_transient_then_succeeds():
    calls = {"n": 0}
    slept: list[float] = []

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("429", status=429)
        return "ok"

    value, outcome = retry_call(
        fn, RetryPolicy(max_attempts=5, base_seconds=0.1, max_seconds=1), sleep=slept.append
    )
    assert value == "ok"
    assert outcome.attempts == 3
    assert len(slept) == 2 == len(outcome.delays)


def test_retry_call_gives_up_after_max_attempts():
    slept: list[float] = []

    def fn():
        raise TransientError("503", status=503)

    with pytest.raises(TransientError):
        retry_call(fn, RetryPolicy(max_attempts=3, base_seconds=0.01), sleep=slept.append)
    assert len(slept) == 2


def test_retry_call_does_not_retry_permanent():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise PermanentError("400", status=400)

    with pytest.raises(PermanentError):
        retry_call(fn, RetryPolicy(max_attempts=5), sleep=lambda _: None)
    assert calls["n"] == 1


def test_retry_after_raises_delay_floor():
    slept: list[float] = []
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientError("429", status=429, retry_after=3)
        return 1

    retry_call(
        fn,
        RetryPolicy(max_attempts=3, base_seconds=0.01, max_seconds=10),
        sleep=slept.append,
        rng=random.Random(1),
    )
    assert slept == [3.0]
