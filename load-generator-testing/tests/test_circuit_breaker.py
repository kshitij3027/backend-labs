"""Unit tests for the CircuitBreaker state machine."""

import asyncio
import time
from unittest.mock import patch

import pytest

from server.circuit_breaker import CircuitBreaker, CircuitState


def _make_breaker(**overrides) -> CircuitBreaker:
    """Create a CircuitBreaker with fast-test defaults."""
    defaults = dict(
        error_threshold=0.5,
        min_requests=5,
        cooldown_secs=0.1,
        recovery_successes=3,
        window_secs=5.0,
    )
    defaults.update(overrides)
    return CircuitBreaker(**defaults)


# ── CLOSED state ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_closed_allows_requests():
    """CLOSED state should allow every request through."""
    cb = _make_breaker()
    for _ in range(10):
        assert await cb.check() is True
    assert cb.state == CircuitState.CLOSED


# ── CLOSED → OPEN transition ────────────────────────────────────


@pytest.mark.asyncio
async def test_closed_to_open_on_high_error_rate():
    """CLOSED should trip to OPEN when error rate >= threshold."""
    cb = _make_breaker(min_requests=5, error_threshold=0.5)

    # Record 5 failures (100 % error rate, above the 50 % threshold)
    for _ in range(5):
        await cb.record_failure()

    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_closed_stays_below_min_requests():
    """CLOSED should NOT trip if fewer than min_requests have been recorded."""
    cb = _make_breaker(min_requests=5, error_threshold=0.5)

    # 4 failures — below min_requests
    for _ in range(4):
        await cb.record_failure()

    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_closed_stays_below_threshold():
    """CLOSED should NOT trip when error rate is below the threshold."""
    cb = _make_breaker(min_requests=5, error_threshold=0.5)

    # 2 failures + 4 successes = 33 % error rate (below 50 %)
    for _ in range(2):
        await cb.record_failure()
    for _ in range(4):
        await cb.record_success()

    assert cb.state == CircuitState.CLOSED


# ── OPEN state ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_open_rejects_requests():
    """OPEN state should reject all requests."""
    cb = _make_breaker(min_requests=5, cooldown_secs=10.0)

    # Trip the breaker
    for _ in range(5):
        await cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # All checks should now return False
    for _ in range(5):
        assert await cb.check() is False


# ── OPEN → HALF_OPEN transition ─────────────────────────────────


@pytest.mark.asyncio
async def test_open_to_half_open_after_cooldown():
    """OPEN should transition to HALF_OPEN once the cooldown expires."""
    cb = _make_breaker(min_requests=5, cooldown_secs=0.1)

    # Trip the breaker
    for _ in range(5):
        await cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # Wait for cooldown
    await asyncio.sleep(0.15)

    # Next check should transition to HALF_OPEN and allow the request
    assert await cb.check() is True
    assert cb.state == CircuitState.HALF_OPEN


# ── HALF_OPEN → CLOSED transition ───────────────────────────────


@pytest.mark.asyncio
async def test_half_open_to_closed_after_recovery():
    """HALF_OPEN should return to CLOSED after enough consecutive successes."""
    cb = _make_breaker(min_requests=5, cooldown_secs=0.1, recovery_successes=3)

    # Trip the breaker
    for _ in range(5):
        await cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # Wait for cooldown and transition to HALF_OPEN
    await asyncio.sleep(0.15)
    assert await cb.check() is True
    assert cb.state == CircuitState.HALF_OPEN

    # Record the required number of successes
    for _ in range(3):
        await cb.record_success()

    assert cb.state == CircuitState.CLOSED


# ── HALF_OPEN → OPEN transition ─────────────────────────────────


@pytest.mark.asyncio
async def test_half_open_to_open_on_failure():
    """HALF_OPEN should trip back to OPEN on any single failure."""
    cb = _make_breaker(min_requests=5, cooldown_secs=0.1, recovery_successes=3)

    # Trip the breaker
    for _ in range(5):
        await cb.record_failure()

    # Wait for cooldown and transition to HALF_OPEN
    await asyncio.sleep(0.15)
    assert await cb.check() is True
    assert cb.state == CircuitState.HALF_OPEN

    # One failure should send it back to OPEN
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN


# ── Window pruning ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_old_failures_pruned_from_window():
    """Failures outside the sliding window should not count toward the rate."""
    cb = _make_breaker(min_requests=5, error_threshold=0.5, window_secs=0.1)

    # Record 5 failures
    for _ in range(5):
        await cb.record_failure()

    # The breaker should be open now
    assert cb.state == CircuitState.OPEN

    # Wait for cooldown, then transition to HALF_OPEN, then recover
    await asyncio.sleep(0.15)
    assert await cb.check() is True  # transitions to HALF_OPEN
    for _ in range(3):
        await cb.record_success()
    assert cb.state == CircuitState.CLOSED

    # Wait so the old failures age out of the window
    await asyncio.sleep(0.15)

    # Now record 5 successes — the old failures are pruned, so error rate is 0
    for _ in range(5):
        await cb.record_success()

    assert cb.state == CircuitState.CLOSED
