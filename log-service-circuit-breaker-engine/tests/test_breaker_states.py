"""State-machine tests for :class:`src.breaker.CircuitBreaker`.

Covers:
    * initial CLOSED state
    * CLOSED stays CLOSED on success
    * CLOSED -> OPEN once failure threshold is hit
    * OPEN -> HALF_OPEN -> CLOSED happy path after recovery_timeout
    * HALF_OPEN failure reopens immediately
    * half_open_max_calls cap is enforced
    * timeouts raise the typed exception and count as failures
    * force_open() works
    * reset() clears state
"""
from __future__ import annotations

import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.exceptions import (
    CircuitBreakerOpenException,
    CircuitBreakerTimeoutException,
)
from src.state import CircuitState


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def make_breaker(**overrides) -> CircuitBreaker:
    """Build a fast-tuned ``CircuitBreaker`` for tests.

    Defaults chosen so the full file runs in well under 5 seconds.
    """
    cfg_kwargs = dict(
        name="test",
        failure_threshold=3,
        recovery_timeout=0.2,
        timeout_duration=0.5,
        half_open_max_calls=2,
        monitoring_window=10.0,
    )
    cfg_kwargs.update(overrides)
    return CircuitBreaker(CircuitBreakerConfig(**cfg_kwargs))


async def succeed() -> str:
    """Fast successful coroutine."""
    return "ok"


async def fail() -> None:
    """Fast failing coroutine that raises a recognized exception type."""
    raise ValueError("boom")


async def slow() -> str:
    """Coroutine that sleeps long enough to overshoot a small timeout."""
    await asyncio.sleep(2.0)
    return "done"


async def _drive_to_open(br: CircuitBreaker) -> None:
    """Push enough failures through ``br`` to trip it OPEN."""
    for _ in range(br.config.failure_threshold):
        with pytest.raises(ValueError):
            await br.call(fail)
    assert br.state == CircuitState.OPEN


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


async def test_starts_closed():
    """A fresh breaker starts in the CLOSED state."""
    br = make_breaker()
    assert br.state == CircuitState.CLOSED
    assert br.get_stats()["total_calls"] == 0


async def test_success_keeps_closed():
    """Five successes do not flip the breaker out of CLOSED."""
    br = make_breaker()
    for _ in range(5):
        result = await br.call(succeed)
        assert result == "ok"
    assert br.state == CircuitState.CLOSED
    stats = br.get_stats()
    assert stats["successful_calls"] == 5
    assert stats["failed_calls"] == 0


async def test_failures_open_circuit():
    """Hitting ``failure_threshold`` failures trips OPEN; next call short-circuits."""
    br = make_breaker()
    await _drive_to_open(br)

    # Subsequent call should short-circuit with the typed exception.
    with pytest.raises(CircuitBreakerOpenException) as exc_info:
        await br.call(succeed)
    assert exc_info.value.breaker_name == "test"
    assert br.state == CircuitState.OPEN


async def test_open_to_half_open_after_recovery_timeout():
    """OPEN -> HALF_OPEN -> CLOSED after recovery + ``half_open_max_calls`` successes."""
    br = make_breaker()
    await _drive_to_open(br)

    # Wait past the recovery window, then send probes.
    await asyncio.sleep(0.25)

    # First probe: state transitions to HALF_OPEN at admission, succeeds,
    # then either remains HALF_OPEN (1 of 2 successes) or closes (2 of 2).
    result = await br.call(succeed)
    assert result == "ok"
    # half_open_max_calls=2, so one success leaves us in HALF_OPEN.
    assert br.state == CircuitState.HALF_OPEN

    # Second probe: completes the recovery; breaker closes.
    result = await br.call(succeed)
    assert result == "ok"
    assert br.state == CircuitState.CLOSED


async def test_half_open_failure_reopens():
    """A single failure during HALF_OPEN slams the breaker back to OPEN."""
    br = make_breaker()
    await _drive_to_open(br)
    await asyncio.sleep(0.25)

    # Probe once to land in HALF_OPEN (1 of 2 successes).
    await br.call(succeed)
    assert br.state == CircuitState.HALF_OPEN

    # A failure during HALF_OPEN reopens immediately.
    with pytest.raises(ValueError):
        await br.call(fail)
    assert br.state == CircuitState.OPEN


async def test_half_open_max_calls_cap():
    """``half_open_max_calls`` controls both success-to-close and inflight cap."""
    br = make_breaker(half_open_max_calls=2)
    await _drive_to_open(br)
    await asyncio.sleep(0.25)

    # Two sequential successes should close the breaker (cap == 2).
    await br.call(succeed)
    assert br.state == CircuitState.HALF_OPEN
    assert br._half_open_successes == 1

    await br.call(succeed)
    assert br.state == CircuitState.CLOSED
    # After CLOSED, half-open bookkeeping is cleared.
    assert br._half_open_inflight == 0
    assert br._half_open_successes == 0


async def test_timeout_raises_typed_exception_and_counts_as_failure():
    """A slow callable raises the timeout exception and increments timeout_calls."""
    br = make_breaker(failure_threshold=10)  # don't trip OPEN on a single timeout
    with pytest.raises(CircuitBreakerTimeoutException) as exc_info:
        await br.call(slow)
    assert exc_info.value.breaker_name == "test"
    assert exc_info.value.timeout_seconds == br.config.timeout_duration

    stats = br.get_stats()
    assert stats["timeout_calls"] >= 1
    assert stats["failed_calls"] >= 1


async def test_force_open_works():
    """``force_open`` flips a healthy breaker to OPEN immediately."""
    br = make_breaker()
    assert br.state == CircuitState.CLOSED
    await br.force_open()
    assert br.state == CircuitState.OPEN
    with pytest.raises(CircuitBreakerOpenException):
        await br.call(succeed)


async def test_reset_clears_state():
    """``reset`` returns the breaker to a fresh CLOSED state with zeroed counters."""
    br = make_breaker()
    await _drive_to_open(br)
    assert br.get_stats()["total_calls"] > 0
    assert br.state == CircuitState.OPEN

    await br.reset()
    stats = br.get_stats()
    assert br.state == CircuitState.CLOSED
    assert stats["total_calls"] == 0
    assert stats["failed_calls"] == 0
    assert stats["successful_calls"] == 0
    assert br._consecutive_failures == 0
