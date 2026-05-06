"""Tests for the :mod:`src.stats` sliding window and per-breaker stats."""
from __future__ import annotations

import pytest

from src.state import CircuitState
from src.stats import CallRecord, CallWindow, CircuitStats


# ---------------------------------------------------------------------------
# CallWindow — recording, trimming, and basic accessors
# ---------------------------------------------------------------------------


def test_call_window_records_and_volume() -> None:
    """5 calls recorded at the same timestamp are all visible inside a wide window."""
    win = CallWindow(window_seconds=60)
    for _ in range(5):
        win.record(success=True, latency=0.05, now=100.0)
    assert win.volume(now=100.0) == 5


def test_call_window_trim_drops_old() -> None:
    """All entries older than ``window_seconds`` are evicted on the next read."""
    win = CallWindow(window_seconds=60)
    for _ in range(3):
        win.record(success=True, latency=0.01, now=10.0)

    # 80 - 60 = 20, all records at t=10 are < cutoff and should be dropped
    assert win.volume(now=80.0) == 0
    assert len(win) == 0


def test_call_window_partial_trim() -> None:
    """Only entries strictly older than the cutoff are evicted; the rest remain."""
    win = CallWindow(window_seconds=60)
    win.record(success=True, latency=0.01, now=10.0)   # expires (10 < 20)
    win.record(success=True, latency=0.01, now=30.0)   # keep (30 >= 20)
    win.record(success=True, latency=0.01, now=70.0)   # keep (70 >= 20)

    assert win.volume(now=80.0) == 2


def test_call_window_explicit_trim_method() -> None:
    """``trim()`` is callable on its own and mutates the underlying deque."""
    win = CallWindow(window_seconds=10)
    win.record(success=True, latency=0.01, now=0.0)
    win.record(success=True, latency=0.01, now=5.0)
    assert len(win) == 2

    win.trim(now=100.0)
    assert len(win) == 0


def test_call_window_clear_drops_everything() -> None:
    """``clear()`` empties the window regardless of timestamps."""
    win = CallWindow(window_seconds=60)
    for _ in range(4):
        win.record(success=True, latency=0.01, now=100.0)
    assert len(win) == 4

    win.clear()
    assert len(win) == 0
    assert win.volume(now=100.0) == 0


# ---------------------------------------------------------------------------
# CallWindow — error rate
# ---------------------------------------------------------------------------


def test_error_rate_zero_with_no_calls() -> None:
    """A fresh window reports zero error rate."""
    win = CallWindow(window_seconds=60)
    assert win.error_rate() == 0.0


def test_error_rate_basic() -> None:
    """Error rate is failures / total within the active window."""
    win = CallWindow(window_seconds=60)
    for _ in range(7):
        win.record(success=True, latency=0.01, now=100.0)
    for _ in range(3):
        win.record(success=False, latency=0.01, now=100.0)

    assert win.error_rate(now=100.0) == pytest.approx(0.3)


def test_error_rate_after_partial_trim() -> None:
    """Old failures rolling out of the window stop counting."""
    win = CallWindow(window_seconds=60)
    # Three failures at t=10 will roll out by t=80 (cutoff=20).
    for _ in range(3):
        win.record(success=False, latency=0.01, now=10.0)
    # Two successes at t=70 remain.
    for _ in range(2):
        win.record(success=True, latency=0.01, now=70.0)

    assert win.error_rate(now=80.0) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# CallWindow — average latency
# ---------------------------------------------------------------------------


def test_avg_latency_basic() -> None:
    """Average latency is the arithmetic mean of in-window records."""
    win = CallWindow(window_seconds=60)
    for lat in (0.1, 0.2, 0.3):
        win.record(success=True, latency=lat, now=100.0)

    assert win.avg_latency(now=100.0) == pytest.approx(0.2)


def test_avg_latency_zero_with_no_calls() -> None:
    """Empty window reports zero average latency, not NaN."""
    win = CallWindow(window_seconds=60)
    assert win.avg_latency() == 0.0


# ---------------------------------------------------------------------------
# CallWindow — input validation
# ---------------------------------------------------------------------------


def test_call_window_init_rejects_zero() -> None:
    """A window of size 0 or negative is nonsensical and rejected."""
    with pytest.raises(ValueError):
        CallWindow(0)
    with pytest.raises(ValueError):
        CallWindow(-1)


# ---------------------------------------------------------------------------
# CallRecord — simple data carrier
# ---------------------------------------------------------------------------


def test_call_record_fields() -> None:
    """``CallRecord`` round-trips its three fields verbatim."""
    rec = CallRecord(timestamp=1.5, success=False, latency=0.42)
    assert rec.timestamp == 1.5
    assert rec.success is False
    assert rec.latency == pytest.approx(0.42)


# ---------------------------------------------------------------------------
# CircuitStats
# ---------------------------------------------------------------------------


def test_circuit_stats_success_rate_zero_calls() -> None:
    """A fresh breaker is vacuously healthy."""
    assert CircuitStats().success_rate() == 1.0


def test_circuit_stats_success_rate_basic() -> None:
    """Success rate is ``successful_calls / total_calls``."""
    stats = CircuitStats(total_calls=10, successful_calls=7)
    assert stats.success_rate() == pytest.approx(0.7)


def test_circuit_stats_defaults() -> None:
    """Defaults match the spec: closed, no calls, no transitions."""
    stats = CircuitStats()
    assert stats.total_calls == 0
    assert stats.successful_calls == 0
    assert stats.failed_calls == 0
    assert stats.timeout_calls == 0
    assert stats.state_changes == 0
    assert stats.last_failure_time is None
    assert stats.last_success_time is None
    assert stats.current_state == CircuitState.CLOSED
    assert stats.opened_at is None
    assert stats.cumulative_open_duration == 0.0


def test_circuit_stats_to_dict_shape() -> None:
    """``to_dict`` exposes every public field plus ``success_rate``."""
    stats = CircuitStats(
        total_calls=10,
        successful_calls=8,
        failed_calls=1,
        timeout_calls=1,
        state_changes=2,
        last_failure_time=12.5,
        last_success_time=15.0,
        current_state=CircuitState.CLOSED,
        opened_at=11.0,
        cumulative_open_duration=4.5,
    )

    d = stats.to_dict()

    expected_keys = {
        "total_calls",
        "successful_calls",
        "failed_calls",
        "timeout_calls",
        "state_changes",
        "last_failure_time",
        "last_success_time",
        "current_state",
        "opened_at",
        "cumulative_open_duration",
        "success_rate",
    }
    assert set(d.keys()) == expected_keys

    # Enum is serialized as its string value, not the Enum instance itself.
    assert d["current_state"] == "CLOSED"
    assert not isinstance(d["current_state"], CircuitState) or isinstance(
        d["current_state"], str
    )

    assert d["cumulative_open_duration"] == pytest.approx(4.5)
    assert d["success_rate"] == pytest.approx(0.8)


def test_circuit_stats_to_dict_open_state_value() -> None:
    """OPEN state also serializes to its plain string value."""
    stats = CircuitStats(current_state=CircuitState.OPEN)
    assert stats.to_dict()["current_state"] == "OPEN"
