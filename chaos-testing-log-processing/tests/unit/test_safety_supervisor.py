"""Unit tests for C15 --- :class:`SafetySupervisor` circuit breaker.

The supervisor is a sync listener on the ``SystemMonitor``: every 5s tick
calls ``on_snapshot``, which either delegates to the async ``_evaluate``
(if an asyncio loop is running) or falls back to the pure-sync
``_evaluate_sync`` path (used by these unit tests). Trip rules are
debounced by ``consecutive_breach_required`` ticks --- a transient spike
must NOT cross the breaker.

We exercise:

* state-machine semantics via ``_evaluate_sync`` (no event loop needed)
* the async fire path (abort + event callback) via ``await _evaluate``
* the ``on_snapshot`` sync-fallback when no loop is running
* the ``ProgressiveBlastRadiusScheduler.next_severity`` clamp.

All mocks are plain ``MagicMock`` / ``AsyncMock`` --- no Docker, no
psutil, no real metric collection. The point of this suite is to nail
down the contract of the *state machine itself*, not to integrate with
the monitor.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from src.engine.supervisor import (
    CircuitBreakerState,
    ProgressiveBlastRadiusScheduler,
    SafetySupervisor,
)
from src.injection.injector import FailureInjector
from src.models.metrics import SystemMetrics


# --------------------------------------------------------------------------- #
# Defaults + helpers
# --------------------------------------------------------------------------- #

CPU_LIMIT = 90.0
MEM_LIMIT = 85.0
MAX_CONC = 3


def _make_injector(active: int = 0) -> MagicMock:
    """A FailureInjector lookalike with a configurable ``active_count`` property.

    ``active_count`` is a property on the real class, so we use
    :class:`PropertyMock` on the *type* of the mock instance to make
    attribute access return our integer.
    """
    mock = MagicMock(spec=FailureInjector)
    type(mock).active_count = PropertyMock(return_value=active)
    return mock


def _make_supervisor(
    *,
    injector: MagicMock | None = None,
    abort_callback=None,
    event_callback=None,
    consecutive: int = 2,
    cpu: float = CPU_LIMIT,
    mem: float = MEM_LIMIT,
    max_conc: int = MAX_CONC,
) -> SafetySupervisor:
    if injector is None:
        injector = _make_injector(active=0)
    if abort_callback is None:
        abort_callback = AsyncMock(return_value=0)
    return SafetySupervisor(
        injector=injector,
        cpu_emergency_threshold_pct=cpu,
        mem_emergency_threshold_pct=mem,
        max_concurrent_scenarios=max_conc,
        abort_callback=abort_callback,
        event_callback=event_callback,
        consecutive_breach_required=consecutive,
    )


def _snap(
    *,
    cpu: float = 10.0,
    mem: float = 10.0,
    disk: float = 10.0,
) -> SystemMetrics:
    """Build a :class:`SystemMetrics` snapshot with simple defaults."""
    return SystemMetrics(cpu_pct=cpu, mem_pct=mem, disk_pct=disk)


# --------------------------------------------------------------------------- #
# Constructor and initial state
# --------------------------------------------------------------------------- #


class TestInitialState:
    def test_fresh_supervisor_has_clean_state(self) -> None:
        sup = _make_supervisor()
        assert isinstance(sup.state, CircuitBreakerState)
        assert sup.state.tripped is False
        assert sup.state.reason is None
        assert sup.state.tripped_at is None
        assert sup.state.last_breach_metric is None
        assert sup.state.consecutive_breach_count == 0
        assert sup.state.total_trips == 0


# --------------------------------------------------------------------------- #
# Debounce semantics (pure _evaluate_sync)
# --------------------------------------------------------------------------- #


class TestDebounce:
    def test_single_breach_does_not_trip(self) -> None:
        sup = _make_supervisor()
        sup._evaluate_sync(_snap(cpu=95.0))
        assert sup.state.consecutive_breach_count == 1
        assert sup.state.tripped is False
        # The latest reason + metric is recorded even between debounce ticks.
        assert sup.state.reason is not None
        assert "cpu" in sup.state.reason.lower()

    def test_two_consecutive_breaches_trip(self) -> None:
        sup = _make_supervisor()
        sup._evaluate_sync(_snap(cpu=95.0))
        sup._evaluate_sync(_snap(cpu=95.0))
        assert sup.state.consecutive_breach_count == 2
        assert sup.state.tripped is True
        assert sup.state.reason is not None
        assert "cpu" in sup.state.reason.lower()
        assert sup.state.last_breach_metric == {
            "name": "cpu_pct",
            "value": 95.0,
            "limit": 90.0,
        }
        assert sup.state.total_trips == 1
        assert isinstance(sup.state.tripped_at, datetime)
        # tz-aware (offset is not None).
        assert sup.state.tripped_at.tzinfo is not None


# --------------------------------------------------------------------------- #
# Transient breach --- one bad tick, one clean tick --- must NOT trip
# --------------------------------------------------------------------------- #


class TestTransientBreach:
    def test_breach_then_clean_resets_counter(self) -> None:
        sup = _make_supervisor()
        sup._evaluate_sync(_snap(cpu=95.0))
        assert sup.state.consecutive_breach_count == 1
        # Clean tick before the debounce threshold is reached.
        sup._evaluate_sync(_snap(cpu=10.0))
        assert sup.state.consecutive_breach_count == 0
        assert sup.state.tripped is False
        assert sup.state.reason is None
        assert sup.state.last_breach_metric is None


# --------------------------------------------------------------------------- #
# Once tripped, clean ticks must not clear the trip state
# --------------------------------------------------------------------------- #


class TestStaysTripped:
    def test_clean_snapshot_after_trip_preserves_state(self) -> None:
        sup = _make_supervisor()
        # Trip the breaker first.
        sup._evaluate_sync(_snap(cpu=95.0))
        sup._evaluate_sync(_snap(cpu=95.0))
        assert sup.state.tripped is True
        tripped_reason = sup.state.reason
        tripped_at = sup.state.tripped_at

        # A clean snapshot afterwards must not undo the trip.
        sup._evaluate_sync(_snap(cpu=10.0))
        assert sup.state.tripped is True
        # The clean-tick guard (`if not tripped:`) means the reason +
        # metric stick around after the trip.
        assert sup.state.reason == tripped_reason
        assert sup.state.tripped_at == tripped_at


# --------------------------------------------------------------------------- #
# Memory breach
# --------------------------------------------------------------------------- #


class TestMemoryBreach:
    def test_memory_threshold_trips_with_mem_reason(self) -> None:
        sup = _make_supervisor()
        # CPU well under, mem at/over threshold.
        sup._evaluate_sync(_snap(cpu=10.0, mem=90.0))
        sup._evaluate_sync(_snap(cpu=10.0, mem=90.0))
        assert sup.state.tripped is True
        assert sup.state.reason is not None
        assert "mem" in sup.state.reason.lower()
        assert sup.state.last_breach_metric is not None
        assert sup.state.last_breach_metric["name"] == "mem_pct"
        assert sup.state.last_breach_metric["value"] == 90.0
        assert sup.state.last_breach_metric["limit"] == MEM_LIMIT


# --------------------------------------------------------------------------- #
# Concurrency breach
# --------------------------------------------------------------------------- #


class TestConcurrencyBreach:
    def test_active_count_at_max_trips_with_active_scenarios_reason(self) -> None:
        injector = _make_injector(active=MAX_CONC)
        sup = _make_supervisor(injector=injector)
        # CPU and mem are both safe, so the only breach path is concurrency.
        sup._evaluate_sync(_snap(cpu=10.0, mem=10.0))
        sup._evaluate_sync(_snap(cpu=10.0, mem=10.0))
        assert sup.state.tripped is True
        assert sup.state.reason is not None
        assert "active_scenarios" in sup.state.reason
        assert sup.state.last_breach_metric is not None
        assert sup.state.last_breach_metric["name"] == "active_scenarios"
        assert sup.state.last_breach_metric["value"] == MAX_CONC
        assert sup.state.last_breach_metric["limit"] == MAX_CONC


# --------------------------------------------------------------------------- #
# Reset clears state but preserves total_trips
# --------------------------------------------------------------------------- #


class TestReset:
    def test_reset_clears_trip_keeps_total_trips(self) -> None:
        sup = _make_supervisor()
        sup._evaluate_sync(_snap(cpu=95.0))
        sup._evaluate_sync(_snap(cpu=95.0))
        assert sup.state.tripped is True
        assert sup.state.total_trips == 1

        returned = sup.reset()

        # Returns the same state object (cleared).
        assert returned is sup.state
        assert sup.state.tripped is False
        assert sup.state.reason is None
        assert sup.state.tripped_at is None
        assert sup.state.last_breach_metric is None
        assert sup.state.consecutive_breach_count == 0
        # total_trips is preserved across reset.
        assert sup.state.total_trips == 1


# --------------------------------------------------------------------------- #
# Async path: abort + event callback fire exactly once on the trip tick
# --------------------------------------------------------------------------- #


class TestAsyncFirePath:
    async def test_two_breaches_fire_abort_once_and_emit_event(self) -> None:
        abort_cb = AsyncMock(return_value=2)
        event_cb = MagicMock()
        sup = _make_supervisor(
            abort_callback=abort_cb,
            event_callback=event_cb,
        )

        await sup._evaluate(_snap(cpu=95.0))
        # First breach: counter goes to 1, not yet tripped -> no abort.
        assert sup.state.tripped is False
        abort_cb.assert_not_called()
        event_cb.assert_not_called()

        await sup._evaluate(_snap(cpu=95.0))
        # Second consecutive breach: trips and fires abort + event.
        assert sup.state.tripped is True
        abort_cb.assert_awaited_once()
        event_cb.assert_called_once()
        args, _kwargs = event_cb.call_args
        event_dict = args[0]
        assert event_dict["event"] == "emergency_stop"
        assert event_dict["aborted_count"] == 2
        assert event_dict["reason"]  # non-empty

        # A third consecutive breach must NOT fire abort again.
        await sup._evaluate(_snap(cpu=95.0))
        abort_cb.assert_awaited_once()  # still just one fire
        # Event also only fired once.
        assert event_cb.call_count == 1

    async def test_transient_breach_async_path_does_not_fire_abort(self) -> None:
        abort_cb = AsyncMock(return_value=0)
        event_cb = MagicMock()
        sup = _make_supervisor(
            abort_callback=abort_cb,
            event_callback=event_cb,
        )
        await sup._evaluate(_snap(cpu=95.0))  # one breach
        await sup._evaluate(_snap(cpu=10.0))  # clean tick before debounce
        assert sup.state.tripped is False
        abort_cb.assert_not_called()
        event_cb.assert_not_called()


# --------------------------------------------------------------------------- #
# on_snapshot sync fallback (no event loop)
# --------------------------------------------------------------------------- #


class TestOnSnapshotSyncFallback:
    def test_on_snapshot_no_loop_runs_sync_path(self) -> None:
        sup = _make_supervisor()
        # Plain sync context --- ``asyncio.get_event_loop`` may either
        # raise or return a non-running loop; in both cases ``on_snapshot``
        # must fall back to ``_evaluate_sync`` and update state.
        sup.on_snapshot(_snap(cpu=95.0))
        assert sup.state.consecutive_breach_count == 1
        assert sup.state.tripped is False


# --------------------------------------------------------------------------- #
# ProgressiveBlastRadiusScheduler clamp
# --------------------------------------------------------------------------- #


class TestProgressiveBlastRadiusScheduler:
    @pytest.mark.parametrize(
        "week, expected",
        [
            (0, 1),
            (1, 1),
            (2, 2),
            (4, 4),
            (99, 4),
            (-5, 1),
        ],
    )
    def test_severity_clamped_to_1_through_4(self, week: int, expected: int) -> None:
        assert ProgressiveBlastRadiusScheduler.next_severity(week) == expected
