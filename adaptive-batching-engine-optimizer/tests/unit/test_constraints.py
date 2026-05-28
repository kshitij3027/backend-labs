"""Unit tests for src.constraints — hard limits, emergency sizing, hysteresis."""

from __future__ import annotations

import pytest

from src.constraints import ConstraintHandler, ConstraintStatus


# --- check(): per-metric breach detection -----------------------------------


def _handler() -> ConstraintHandler:
    """Handler pinned to the spec defaults (independent of any env overrides)."""
    return ConstraintHandler(
        cpu_threshold=90.0,
        memory_threshold=90.0,
        latency_threshold=1000.0,
        recovery_cpu=70.0,
        recovery_memory=70.0,
        recovery_latency=300.0,
        recovery_cycles=3,
        min_batch_size=50,
    )


def test_check_no_breach_when_all_under_thresholds() -> None:
    status = _handler().check(cpu_percent=50.0, memory_percent=60.0, latency_ms=200.0)

    assert isinstance(status, ConstraintStatus)
    assert status.breach is False
    assert status.cpu_breach is False
    assert status.memory_breach is False
    assert status.latency_breach is False
    assert status.reason == "ok"


def test_check_cpu_breach_when_cpu_above_threshold() -> None:
    status = _handler().check(cpu_percent=95.0, memory_percent=60.0, latency_ms=200.0)

    assert status.breach is True
    assert status.cpu_breach is True
    assert status.memory_breach is False
    assert status.latency_breach is False
    assert "cpu" in status.reason
    assert "memory" not in status.reason
    assert "latency" not in status.reason


def test_check_cpu_at_exact_threshold_is_not_a_breach() -> None:
    """Boundary: a strict ``>`` comparison means 90.0 == 90.0 is healthy."""
    status = _handler().check(cpu_percent=90.0, memory_percent=90.0, latency_ms=1000.0)

    assert status.breach is False
    assert status.cpu_breach is False
    assert status.memory_breach is False
    assert status.latency_breach is False
    assert status.reason == "ok"


def test_check_memory_breach_when_memory_above_threshold() -> None:
    status = _handler().check(cpu_percent=50.0, memory_percent=95.0, latency_ms=200.0)

    assert status.breach is True
    assert status.cpu_breach is False
    assert status.memory_breach is True
    assert status.latency_breach is False
    assert "memory" in status.reason
    assert "cpu" not in status.reason


def test_check_latency_breach_when_latency_above_threshold() -> None:
    status = _handler().check(cpu_percent=50.0, memory_percent=60.0, latency_ms=1200.0)

    assert status.breach is True
    assert status.cpu_breach is False
    assert status.memory_breach is False
    assert status.latency_breach is True
    assert "latency" in status.reason


def test_check_breach_true_if_any_metric_breaches() -> None:
    # Only memory over the line, everything else healthy.
    assert _handler().check(50.0, 91.0, 200.0).breach is True
    # Only latency over the line.
    assert _handler().check(50.0, 60.0, 1001.0).breach is True


def test_check_reason_mentions_all_breached_metrics() -> None:
    status = _handler().check(cpu_percent=99.0, memory_percent=99.0, latency_ms=2000.0)

    assert status.breach is True
    assert status.cpu_breach is True
    assert status.memory_breach is True
    assert status.latency_breach is True
    assert "cpu" in status.reason
    assert "memory" in status.reason
    assert "latency" in status.reason


def test_is_breach_matches_check_breach() -> None:
    handler = _handler()
    assert handler.is_breach(50.0, 60.0, 200.0) is False
    assert handler.is_breach(95.0, 60.0, 200.0) is True
    assert handler.is_breach(95.0, 60.0, 200.0) == handler.check(95.0, 60.0, 200.0).breach


def test_check_uses_settings_defaults_when_thresholds_none() -> None:
    """With no overrides the handler falls back to the spec defaults (90/90/1000)."""
    handler = ConstraintHandler()
    # 90.0 is the default boundary -> not a breach (strict >).
    assert handler.check(90.0, 90.0, 1000.0).breach is False
    # Just over each default -> breach.
    assert handler.check(90.1, 50.0, 100.0).cpu_breach is True
    assert handler.check(50.0, 90.1, 100.0).memory_breach is True
    assert handler.check(50.0, 50.0, 1000.1).latency_breach is True


# --- emergency_batch_size() --------------------------------------------------


def test_emergency_batch_size_halves_by_default() -> None:
    handler = _handler()  # emergency_reduction_factor defaults to 0.5
    assert handler.emergency_batch_size(1000) == 500


def test_emergency_batch_size_floors_at_min_batch_size() -> None:
    handler = _handler()  # min_batch_size=50, factor 0.5
    # 60 * 0.5 = 30, which is below the 50 floor -> clamps to 50.
    assert handler.emergency_batch_size(60) == 50


def test_emergency_batch_size_respects_custom_reduction_factor() -> None:
    handler = ConstraintHandler(min_batch_size=50, emergency_reduction_factor=0.25)
    # 1000 * 0.25 = 250 (well above the floor).
    assert handler.emergency_batch_size(1000) == 250
    # 100 * 0.25 = 25 -> below floor -> 50.
    assert handler.emergency_batch_size(100) == 50


def test_emergency_batch_size_truncates_toward_zero() -> None:
    handler = _handler()
    # 999 * 0.5 = 499.5 -> int() truncates to 499.
    assert handler.emergency_batch_size(999) == 499


# --- recovery hysteresis -----------------------------------------------------


def test_fresh_handler_is_not_recovery_ready() -> None:
    assert _handler().recovery_ready() is False


def test_recovery_ready_after_enough_consecutive_healthy_cycles() -> None:
    handler = _handler()  # recovery_cycles=3, recovery thresholds 70/70/300
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is False  # 1 healthy cycle
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is False  # 2 healthy cycles
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is True  # 3rd healthy cycle reaches the bar


def test_unhealthy_cycle_resets_the_streak() -> None:
    handler = _handler()
    handler.note_cycle(50.0, 50.0, 100.0)
    handler.note_cycle(50.0, 50.0, 100.0)
    # An unhealthy cycle (cpu above recovery threshold) wipes the streak.
    handler.note_cycle(75.0, 50.0, 100.0)
    assert handler.recovery_ready() is False
    # Must rebuild the full streak from scratch.
    handler.note_cycle(50.0, 50.0, 100.0)
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is False
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is True


def test_value_in_hysteresis_gap_counts_as_not_healthy() -> None:
    """A metric below the breach threshold but above recovery is NOT healthy.

    cpu=75 is below the breach threshold (90) -> ``check`` reports no breach,
    yet it sits above the recovery threshold (70), so it must not advance the
    healthy streak. This dead band is what prevents EMERGENCY<->OPTIMIZING flap.
    """
    handler = _handler()
    # Confirm 75 is in the gap: not a breach...
    assert handler.is_breach(75.0, 50.0, 100.0) is False
    # ...but it never builds a healthy streak no matter how many cycles.
    for _ in range(handler.recovery_cycles + 2):
        handler.note_cycle(75.0, 50.0, 100.0)
    assert handler.recovery_ready() is False


def test_recovery_threshold_is_strict() -> None:
    """Exactly at the recovery threshold is NOT healthy (strict ``<``)."""
    handler = _handler()  # recovery_cpu=70.0
    for _ in range(handler.recovery_cycles):
        handler.note_cycle(70.0, 50.0, 100.0)
    assert handler.recovery_ready() is False


def test_reset_zeroes_the_streak() -> None:
    handler = _handler()
    handler.note_cycle(50.0, 50.0, 100.0)
    handler.note_cycle(50.0, 50.0, 100.0)
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is True

    handler.reset()
    assert handler.recovery_ready() is False

    # After reset it takes the full streak again.
    handler.note_cycle(50.0, 50.0, 100.0)
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is False
    handler.note_cycle(50.0, 50.0, 100.0)
    assert handler.recovery_ready() is True


def test_recovery_uses_settings_defaults_when_none() -> None:
    """Default recovery_cycles is 3 per settings."""
    handler = ConstraintHandler()
    for _ in range(2):
        handler.note_cycle(10.0, 10.0, 10.0)
    assert handler.recovery_ready() is False
    handler.note_cycle(10.0, 10.0, 10.0)
    assert handler.recovery_ready() is True
