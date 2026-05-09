"""Tests for last/avg recovery duration tracking on the CircuitBreaker."""
from __future__ import annotations
import asyncio
import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig


def _cfg(name: str = "test_recovery") -> CircuitBreakerConfig:
    return CircuitBreakerConfig(
        name=name,
        failure_threshold=2,
        recovery_timeout=0.1,
        timeout_duration=0.5,
        half_open_max_calls=2,
        # Very short window so stale failures from a previous cycle age out
        # before the next cycle's _bad calls run; otherwise cycle 2's first
        # call would re-trip an already-open accounting.
        monitoring_window=0.05,
        consecutive_failures_threshold=2,
        min_volume_threshold=2,
    )


async def _ok():
    return "ok"


async def _bad():
    raise RuntimeError("boom")


async def _drive_open_close_cycle(breaker: CircuitBreaker, sleep_seconds: float = 0.15) -> None:
    """Drive a single full OPEN -> HALF_OPEN -> CLOSED recovery cycle."""
    # Trip the breaker open with two failures.
    for _ in range(2):
        try:
            await breaker.call(_bad)
        except RuntimeError:
            pass

    # Wait past recovery_timeout, then run two successful probes (half_open_max_calls=2).
    await asyncio.sleep(sleep_seconds)
    await breaker.call(_ok)
    await breaker.call(_ok)


async def test_recovery_duration_tracked_after_close():
    breaker = CircuitBreaker(_cfg("recovery_one"))

    await _drive_open_close_cycle(breaker, sleep_seconds=0.15)

    last = breaker.last_recovery_duration
    assert last is not None
    # Allow generous tolerance for scheduling jitter on busy CI hosts.
    assert 0.10 <= last <= 0.50, f"unexpected last_recovery_duration={last!r}"

    # Single cycle => avg equals last.
    assert breaker.avg_recovery_duration == last


async def test_avg_recovery_duration_across_3_cycles():
    breaker = CircuitBreaker(_cfg("recovery_three"))

    durations = []
    for i in range(3):
        if i > 0:
            # Let any failures recorded in the previous cycle's window age
            # out so cycle N starts from a clean call window.
            await asyncio.sleep(0.06)
        await _drive_open_close_cycle(breaker, sleep_seconds=0.15)
        durations.append(breaker.last_recovery_duration)

    assert len(breaker._recovery_durations) == 3
    expected_avg = sum(durations) / 3
    actual_avg = breaker.avg_recovery_duration
    assert actual_avg is not None
    assert abs(actual_avg - expected_avg) < 1e-6


async def test_to_dict_includes_recovery_fields():
    breaker = CircuitBreaker(_cfg("recovery_fresh"))

    snapshot = breaker.to_dict()

    assert "last_recovery_duration" in snapshot
    assert "avg_recovery_duration" in snapshot
    assert snapshot["last_recovery_duration"] is None
    assert snapshot["avg_recovery_duration"] is None
