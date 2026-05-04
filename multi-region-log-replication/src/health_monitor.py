"""Periodic health snapshotter + automatic failover.

The :class:`HealthMonitor` is the bridge between the live cluster state
(``Region`` objects + :class:`ReplicationController` + replication stats)
and the wire format consumed by ``GET /api/health``, ``GET /api/status``,
and the ``/ws`` broadcaster — a :class:`HealthSnapshot`.

Responsibilities:

* Run a background asyncio task that calls :meth:`compute_snapshot`
  every ``health_check_interval_sec`` seconds, caching the latest
  result on ``_last_snapshot`` for cheap debugging access.
* Detect primary failure: if the elected primary reports ``is_healthy
  is False`` for **two consecutive ticks**, we re-elect a new primary
  (excluding the current one) and append a structured event to the
  bounded ``failover_history`` deque.
* Expose :meth:`get_snapshot` for synchronous callers (the HTTP routes
  and the WS broadcaster) — always returns a fresh snapshot so even if
  the background task hasn't ticked yet (e.g. during the very first
  request after startup) the caller still gets up-to-date numbers.

Why two ticks (not one):
  A single missed tick could be a transient hiccup or a race with a
  partial mark_offline that gets reverted. Two consecutive ticks
  with the default 1-second cadence trips failover within ~2s of a
  real outage — comfortably inside the project's 5s recovery budget
  while still smoothing out a brief blip.

Why all exceptions in ``_run`` are swallowed (and logged):
  The monitor must never crash — if ``compute_snapshot`` or the
  failover branch raises, the background task should log + continue,
  not exit. We use ``logger.exception`` so the stack trace is
  preserved in the logs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from .models import HealthSnapshot, RegionStatus
from .region import Region
from .replication_controller import ReplicationController
from .replication_stats import ReplicationStatsTracker


class HealthMonitor:
    """Periodically snapshot cluster health + drive automatic failover."""

    # How many consecutive unhealthy ticks the primary must show before we
    # re-elect. Two ticks at the default 1s cadence means failover fires
    # within ~2s of the simulated outage — well inside the 5s budget.
    UNHEALTHY_THRESHOLD: int = 2

    # Maximum number of past failover events to keep on the snapshot.
    FAILOVER_HISTORY_MAXLEN: int = 10

    def __init__(
        self,
        regions: Dict[str, Region],
        controller: ReplicationController,
        stats: ReplicationStatsTracker,
        check_interval_sec: float,
    ) -> None:
        self._regions: Dict[str, Region] = regions
        self._controller: ReplicationController = controller
        self._stats: ReplicationStatsTracker = stats
        self._interval: float = check_interval_sec
        self._task: Optional[asyncio.Task[None]] = None
        self._last_snapshot: Optional[HealthSnapshot] = None
        # Failover bookkeeping. ``_unhealthy_streak`` lives on the
        # instance (rather than a local in ``_run``) so tests can poke
        # at it during diagnostic failures, and so a future feature
        # could surface it on the snapshot if needed.
        self._unhealthy_streak: int = 0
        self._failover_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.FAILOVER_HISTORY_MAXLEN
        )
        self._logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spin up the background polling task (idempotent)."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the background task and wait for it to exit cleanly."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """The polling loop — never crashes, sleeps between iterations.

        Each tick:

        1. Inspect the elected primary. If it's unhealthy, bump the
           consecutive-unhealthy counter; once that counter reaches
           ``UNHEALTHY_THRESHOLD`` we trigger ``_do_failover`` and reset.
        2. Recompute the snapshot for cheap debugging access.
        """
        while True:
            try:
                current_primary_id = self._controller.primary_id
                if current_primary_id is not None:
                    primary_region = self._regions.get(current_primary_id)
                    if primary_region is not None and not primary_region.is_healthy:
                        self._unhealthy_streak += 1
                        if self._unhealthy_streak >= self.UNHEALTHY_THRESHOLD:
                            await self._do_failover(current_primary_id)
                            self._unhealthy_streak = 0
                    else:
                        # Primary is healthy — clear the streak so a
                        # transient blip doesn't accumulate.
                        self._unhealthy_streak = 0
                self._last_snapshot = self.compute_snapshot()
            except Exception as exc:  # noqa: BLE001 — monitor must never die
                self._logger.exception("health monitor tick failed: %s", exc)
            await asyncio.sleep(self._interval)

    async def _do_failover(self, old_primary_id: str) -> None:
        """Re-elect a primary excluding the failed one; record the event.

        Emits a structured warning log line and appends a dict to
        ``_failover_history``. If election raises (no healthy region
        available) we log an error and leave the stale primary id in
        place — the next tick will retry once a region recovers.
        """
        start = time.perf_counter()
        try:
            new_primary_id = self._controller.elect_primary(
                exclude={old_primary_id}
            )
        except RuntimeError:
            self._logger.error(
                "failover: no healthy region available, leaving %s as primary",
                old_primary_id,
            )
            return
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        self._logger.warning(
            "failover: %s -> %s in %.2fms",
            old_primary_id,
            new_primary_id,
            elapsed_ms,
        )
        self._failover_history.append(
            {
                "at": time.time(),
                "old_primary": old_primary_id,
                "new_primary": new_primary_id,
                "elapsed_ms": elapsed_ms,
            }
        )

    # ------------------------------------------------------------------
    # Snapshot construction
    # ------------------------------------------------------------------

    def failover_events(self) -> List[Dict[str, Any]]:
        """Return a list copy of recent failover events (oldest first)."""
        return list(self._failover_history)

    def compute_snapshot(self) -> HealthSnapshot:
        """Build a :class:`HealthSnapshot` from live cluster state.

        Surfacing rules:

        * ``replication_lag_ms`` reports the **p95** of the secondary's
          rolling window (we expose p95 as the headline lag because it's
          the most decision-relevant percentile per ``plan.md``). The
          primary entry will report its own p95 too — that's just the
          lag of writes *to* the primary's sample bucket, which is a
          no-op (primary doesn't replicate to itself), so it stays at 0.0.
        * ``replication_success_rate`` is the simple
          ``successes / (successes + failures)`` ratio.
        * ``overall_status``:

          - ``"down"`` if no primary has been elected (controller is
            unable to find a healthy candidate).
          - ``"degraded"`` if any region is unhealthy.
          - ``"healthy"`` otherwise.
        """
        stats_snap = self._stats.snapshot()
        primary_id = self._controller.primary_id

        regions_out: List[RegionStatus] = []
        for region_id, region in self._regions.items():
            rstats = stats_snap.get(region_id, {})
            regions_out.append(
                RegionStatus(
                    region_id=region_id,
                    is_primary=(region_id == primary_id),
                    is_healthy=region.is_healthy,
                    log_count=len(region.log_store),
                    vector_clock=dict(region.vector_clock),
                    logical_ts=region.logical_ts,
                    replication_lag_ms=rstats.get("p95"),
                    replication_success_rate=rstats.get("success_rate"),
                )
            )

        # Overall status — see method docstring.
        if primary_id is None:
            overall = "down"
        elif any(not r.is_healthy for r in self._regions.values()):
            overall = "degraded"
        else:
            overall = "healthy"

        return HealthSnapshot(
            overall_status=overall,
            regions=regions_out,
            taken_at=time.time(),
            current_primary=primary_id,
            recent_failovers=self.failover_events(),
        )

    def get_snapshot(self) -> HealthSnapshot:
        """Return a fresh snapshot, also caching it on ``_last_snapshot``.

        Always recomputes — see module docstring. The cached
        ``_last_snapshot`` is kept around as a debugging aid (and for
        the failover heuristics in :meth:`_run`) but is *not* what gets
        returned here.
        """
        snap = self.compute_snapshot()
        self._last_snapshot = snap
        return snap
