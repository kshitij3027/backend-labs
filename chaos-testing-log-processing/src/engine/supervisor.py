"""Background safety supervisor --- kill switch + circuit breaker.

Subscribes to the SystemMonitor and triggers a global abort on any of:

- CPU% over the configured emergency threshold
- Memory% over the configured emergency threshold
- Active scenario count at or above ``max_concurrent_scenarios``

Trigger is debounced: a single transient spike does not trip the
breaker. ``consecutive_breach_required`` consecutive snapshots must
breach before we fire. Once tripped, the breaker stays tripped until
``reset()`` is called by an operator (e.g., via the admin REST endpoint).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from ..injection.injector import FailureInjector
from ..models.metrics import SystemMetrics
from ..observability.prom import EMERGENCY_STOPS_TOTAL

logger = logging.getLogger(__name__)


@dataclass
class CircuitBreakerState:
    """Mutable state describing the supervisor's current posture.

    Persistent across resets only insofar as ``total_trips`` keeps the
    historical counter — every other field is cleared on :meth:`reset`.
    """

    tripped: bool = False
    reason: Optional[str] = None
    tripped_at: Optional[datetime] = None
    last_breach_metric: Optional[dict] = None
    consecutive_breach_count: int = 0
    total_trips: int = 0

    def to_dict(self) -> dict:
        """JSON-friendly representation for the admin REST endpoint."""
        return {
            "tripped": self.tripped,
            "reason": self.reason,
            "tripped_at": self.tripped_at.isoformat() if self.tripped_at else None,
            "last_breach_metric": self.last_breach_metric,
            "consecutive_breach_count": self.consecutive_breach_count,
            "total_trips": self.total_trips,
        }


class SafetySupervisor:
    """Pulls metric samples from a SystemMonitor listener and gates runs.

    The monitor calls :meth:`on_snapshot` synchronously on every tick.
    We hand off to an async :meth:`_evaluate` helper so the abort + event
    emission can be ``await``ed without blocking the monitor's loop.

    Trip rules (any one trips, evaluated in order):
        1. ``snap.cpu_pct >= cpu_emergency_threshold_pct``
        2. ``snap.mem_pct >= mem_emergency_threshold_pct``
        3. ``injector.active_count >= max_concurrent_scenarios``

    Debounce: a snapshot that breaches resets-or-increments the
    consecutive counter; a clean snapshot resets it back to zero
    *unless* we are already tripped, in which case the breach state is
    preserved and only :meth:`reset` clears it.

    Once tripped, the breaker stays tripped — no auto-recovery, per the
    spec's "human in the loop after a breach" requirement.
    """

    def __init__(
        self,
        injector: FailureInjector,
        cpu_emergency_threshold_pct: float,
        mem_emergency_threshold_pct: float,
        max_concurrent_scenarios: int,
        abort_callback,  # async callable: () -> Awaitable[int]
        event_callback=None,  # optional async or sync callable: (event_dict) -> None
        consecutive_breach_required: int = 2,
    ) -> None:
        self._injector = injector
        self._cpu_limit = float(cpu_emergency_threshold_pct)
        self._mem_limit = float(mem_emergency_threshold_pct)
        self._max_concurrent = int(max_concurrent_scenarios)
        self._abort = abort_callback
        self._event = event_callback
        self._needed = int(consecutive_breach_required)
        self.state = CircuitBreakerState()

    # -- snapshot hook (registered via SystemMonitor.add_listener) ----

    def on_snapshot(self, snap: SystemMetrics) -> None:
        """Sync listener; schedules the async _evaluate as a fire-and-forget task.

        The SystemMonitor invokes listeners on every tick (5s by default).
        We must not block the monitor's per-tick fanout, so we delegate to
        an async helper via ``loop.create_task``. If we're outside an
        asyncio context (e.g., a unit test that drives the listener
        directly without a running loop), fall back to a pure sync
        evaluation — the test seam is intentional.
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop in this thread at all -> sync path.
            self._evaluate_sync(snap)
            return

        if loop.is_running():
            loop.create_task(self._evaluate(snap))
        else:
            # Loop exists but isn't running (rare; typically unit tests
            # constructing a fresh loop). Use the sync path so state is
            # still observable to assertions.
            self._evaluate_sync(snap)

    async def _evaluate(self, snap: SystemMetrics) -> None:
        """Async breach-evaluation path.

        Runs ``_evaluate_sync`` first (pure decision logic) and then,
        only if the breaker JUST tripped on this tick, fires the abort
        callback and emits the ``emergency_stop`` event. The "JUST
        tripped" check uses ``consecutive_breach_count == self._needed``
        — past trips have a counter that has continued to climb above
        ``self._needed`` and so do not re-fire the abort.
        """
        await asyncio.sleep(0)  # let the event loop breathe
        already_tripped_before = self.state.tripped
        self._evaluate_sync(snap)

        # Fire abort + event exactly once: on the tick we cross from
        # untripped -> tripped. Subsequent breaches after we are already
        # tripped do nothing (no re-fire).
        just_tripped = self.state.tripped and not already_tripped_before
        if not just_tripped:
            return

        try:
            count = await self._abort()
            # Bump the prom counter ONCE per transition tick (gated by
            # ``just_tripped`` above) so dashboards see a clean trip rate.
            EMERGENCY_STOPS_TOTAL.inc()
            logger.warning(
                "emergency stop fired: aborted=%s reason=%s",
                count,
                self.state.reason,
            )
            await self._emit_event(aborted_count=count)
        except Exception:
            logger.exception("emergency abort failed")

    def _evaluate_sync(self, snap: SystemMetrics) -> None:
        """Pure decision logic --- easy to unit-test.

        No I/O, no callbacks; only mutates ``self.state``. The async
        wrapper :meth:`_evaluate` is responsible for invoking the abort
        callback and emitting the event on the transition tick.
        """
        breach_reason: Optional[str] = None
        metric_payload: Optional[dict] = None

        if snap.cpu_pct >= self._cpu_limit:
            breach_reason = (
                f"cpu_pct {snap.cpu_pct:.1f} >= {self._cpu_limit:.1f}"
            )
            metric_payload = {
                "name": "cpu_pct",
                "value": snap.cpu_pct,
                "limit": self._cpu_limit,
            }
        elif snap.mem_pct >= self._mem_limit:
            breach_reason = (
                f"mem_pct {snap.mem_pct:.1f} >= {self._mem_limit:.1f}"
            )
            metric_payload = {
                "name": "mem_pct",
                "value": snap.mem_pct,
                "limit": self._mem_limit,
            }
        elif self._injector.active_count >= self._max_concurrent:
            breach_reason = (
                f"active_scenarios {self._injector.active_count} "
                f">= {self._max_concurrent}"
            )
            metric_payload = {
                "name": "active_scenarios",
                "value": self._injector.active_count,
                "limit": self._max_concurrent,
            }

        if breach_reason is None:
            # Clean tick --- reset the consecutive counter (unless we
            # are already tripped, in which case we preserve the trip
            # state until an operator calls ``reset()``).
            if not self.state.tripped:
                self.state.consecutive_breach_count = 0
                self.state.last_breach_metric = None
                self.state.reason = None
            return

        # Breach observed this tick. Always record the latest metric +
        # reason so the admin endpoint shows something useful even
        # between debounce ticks.
        self.state.consecutive_breach_count += 1
        self.state.reason = breach_reason
        self.state.last_breach_metric = metric_payload

        if (
            self.state.consecutive_breach_count >= self._needed
            and not self.state.tripped
        ):
            self.state.tripped = True
            self.state.tripped_at = datetime.now(timezone.utc)
            self.state.total_trips += 1

    async def _emit_event(self, *, aborted_count: int) -> None:
        """Push the ``emergency_stop`` event onto the engine event queue.

        ``self._event`` may be either a sync or async callable. We
        await the result iff it's a coroutine. All exceptions are
        logged-and-swallowed so a misbehaving event sink can never
        prevent the abort from completing.
        """
        if self._event is None:
            return
        event = {
            "event": "emergency_stop",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "reason": self.state.reason,
            "metric": self.state.last_breach_metric,
            "aborted_count": aborted_count,
            "total_trips": self.state.total_trips,
        }
        try:
            result = self._event(event)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception("event callback failed")

    # -- operator API --------------------------------------------------

    def reset(self) -> CircuitBreakerState:
        """Clear the tripped state (operator action).

        Preserves ``total_trips`` so the historical counter survives a
        reset --- every other field returns to its initial value.
        """
        logger.info(
            "circuit breaker reset (previous reason=%s)", self.state.reason
        )
        self.state.tripped = False
        self.state.reason = None
        self.state.tripped_at = None
        self.state.last_breach_metric = None
        self.state.consecutive_breach_count = 0
        return self.state


class ProgressiveBlastRadiusScheduler:
    """Stub for the campaign-scaling feature; not driven by anything yet.

    The full feature ramps scenario severity over a multi-week
    campaign so faults grow progressively more disruptive. For the MVP
    we expose only :meth:`next_severity` so other components can be
    wired against the API without committing to the full scheduler.

    Severity returns ``1..4`` for weeks 1..4; values outside that range
    are clamped (``<= 0`` -> ``1``, ``>= 4`` -> ``4``).
    """

    @staticmethod
    def next_severity(week_index: int) -> int:
        if week_index <= 0:
            return 1
        return min(max(week_index, 1), 4)
