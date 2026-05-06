"""Tests for typed-timeout exception handling in CircuitBreaker."""
from __future__ import annotations

import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.exceptions import CircuitBreakerTimeoutException
from src.state import CircuitState


def make_br(**overrides) -> CircuitBreaker:
    """Build a breaker with very short timeouts for fast tests."""
    cfg_kwargs = dict(
        name="t-to",
        failure_threshold=999,
        recovery_timeout=10.0,
        timeout_duration=0.05,
        half_open_max_calls=2,
        monitoring_window=60.0,
        error_rate_threshold=2.0,
        slow_call_duration_threshold=10.0,
        consecutive_failures_threshold=999,
        min_volume_threshold=999,
    )
    cfg_kwargs.update(overrides)
    return CircuitBreaker(CircuitBreakerConfig(**cfg_kwargs))


async def _slow() -> str:
    """Sleep long enough to overshoot a 50ms timeout."""
    await asyncio.sleep(0.5)
    return "done"


async def _fast() -> str:
    return "ok"


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


async def test_timeout_raises_typed_exception():
    """A slow call surfaces ``CircuitBreakerTimeoutException`` with attrs set."""
    br = make_br()

    with pytest.raises(CircuitBreakerTimeoutException) as exc_info:
        await br.call(_slow)

    assert exc_info.value.breaker_name == "t-to"
    assert exc_info.value.timeout_seconds == br.config.timeout_duration


async def test_timeout_counts_as_failure():
    """Three timeouts in a row trip the breaker via the consecutive rule."""
    br = make_br(
        failure_threshold=3,
        consecutive_failures_threshold=3,
        min_volume_threshold=3,
    )

    for _ in range(3):
        with pytest.raises(CircuitBreakerTimeoutException):
            await br.call(_slow)

    assert br.state == CircuitState.OPEN
    stats = br.get_stats()
    assert stats["timeout_calls"] >= 3
    assert stats["failed_calls"] >= 3


async def test_normal_call_after_timeout_resets_consecutive():
    """A success following a timeout resets the consecutive_failures counter."""
    br = make_br(consecutive_failures_threshold=10)  # don't trip during the test

    with pytest.raises(CircuitBreakerTimeoutException):
        await br.call(_slow)
    assert br._consecutive_failures == 1

    result = await br.call(_fast)
    assert result == "ok"
    assert br._consecutive_failures == 0
    assert br.state == CircuitState.CLOSED
