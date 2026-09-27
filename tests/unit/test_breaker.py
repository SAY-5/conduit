"""Circuit breaker: open on consecutive target failures, probe once, close on success."""

from __future__ import annotations

import random

import pytest
from conduit.core.breaker import BreakerState, CircuitBreaker

from tests.unit.test_ratelimit import Clock


def breaker(threshold: int = 3, recovery: float = 10.0) -> tuple[CircuitBreaker, Clock]:
    clock = Clock()
    return CircuitBreaker(threshold, recovery, clock=clock), clock


def test_closed_until_the_threshold_is_reached():
    cb, _ = breaker(threshold=3)
    assert [cb.record_failure() for _ in range(3)] == [False, False, True]
    assert cb.state is BreakerState.OPEN
    assert cb.opens == 1
    assert not cb.allow()


def test_a_success_clears_the_failure_run():
    cb, _ = breaker(threshold=3)
    cb.record_failure()
    cb.record_failure()
    assert cb.record_success() is False
    assert cb.record_failure() is False
    assert cb.state is BreakerState.CLOSED


def test_half_open_admits_one_probe_and_closes_on_success():
    cb, clock = breaker(threshold=1, recovery=10.0)
    cb.record_failure()
    assert cb.remaining() == 10.0
    clock.advance(9.9)
    assert not cb.allow() and cb.state is BreakerState.OPEN
    clock.advance(0.1)
    assert cb.state is BreakerState.HALF_OPEN and cb.remaining() == 0.0
    assert cb.allow() is True
    assert cb.allow() is False
    assert cb.record_success() is True
    assert cb.state is BreakerState.CLOSED and cb.allow()


def test_a_failed_probe_opens_for_another_window():
    cb, clock = breaker(threshold=5, recovery=10.0)
    for _ in range(5):
        cb.record_failure()
    clock.advance(10)
    assert cb.allow()
    assert cb.record_failure() is True
    assert cb.state is BreakerState.OPEN
    assert cb.remaining() == 10.0
    assert cb.opens == 2


def test_an_abandoned_probe_lets_the_next_delivery_probe():
    cb, clock = breaker(threshold=1, recovery=10.0)
    cb.record_failure()
    clock.advance(10)
    assert cb.allow() and cb.probing
    assert cb.allow() is False
    cb.abandon_probe()
    assert cb.state is BreakerState.HALF_OPEN and not cb.probing
    assert cb.allow() is True


def test_any_outcome_sequence_leaves_the_breaker_able_to_settle():
    """Random walk over the transitions: closed always admits, open never does, and a
    half-open breaker admits exactly one probe whenever none is in flight."""
    rng = random.Random(7)
    cb, clock = breaker(threshold=3, recovery=10.0)
    seen = set()
    for _ in range(5000):
        step = rng.choice(["success", "failure", "abandon", "advance", "allow"])
        if step == "success":
            cb.record_success()
        elif step == "failure":
            cb.record_failure()
        elif step == "abandon":
            cb.abandon_probe()
        elif step == "advance":
            clock.advance(rng.choice([1.0, 5.0, 10.0]))
        else:
            cb.allow()
        state = cb.state
        seen.add((state, cb.probing))
        if state is BreakerState.CLOSED:
            assert cb.allow() and not cb.probing
        elif state is BreakerState.OPEN:
            assert not cb.allow() and not cb.probing
        elif cb.probing:
            assert cb.allow() is False
        else:
            assert cb.allow() is True and cb.probing
            cb.abandon_probe()
    assert (BreakerState.HALF_OPEN, True) in seen and (BreakerState.OPEN, False) in seen


def test_settings_are_validated():
    with pytest.raises(ValueError):
        CircuitBreaker(0, 10)
    with pytest.raises(ValueError):
        CircuitBreaker(1, 0)
