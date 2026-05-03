"""Periodic health snapshotter.

The :class:`HealthMonitor` is the bridge between the live cluster state
(``Region`` objects + :class:`ReplicationController` + replication stats)
and the wire format consumed by ``GET /api/health``, ``GET /api/status``,
and the ``/ws`` broadcaster — a :class:`HealthSnapshot`.

Responsibilities (commit 4 — failover detection lands in commit 5):

* Run a background asyncio task that calls :meth:`compute_snapshot`
  every ``health_check_interval_sec`` seconds, caching the latest
  result on ``_last_snapshot`` for cheap debugging access.
* Expose :meth:`get_snapshot` for synchronous callers (the HTTP routes
  and the WS broadcaster) — always returns a fresh snapshot so even if
  the background task hasn't ticked yet (e.g. during the very first
  request after startup) the caller still gets up-to-date numbers.

Why we re-compute on every ``get_snapshot``:
  The cost is tiny (three regions × one stats dict lookup), and it
  removes a subtle staleness window where the dashboard would render
  values up to ``health_check_interval_sec`` old. Commit 5 will keep
  the cached ``_last_snapshot`` for failover decisions which need to
  observe state changes between ticks.

Why all exceptions in ``_run`` are swallowed:
  The monitor must never crash — if ``compute_snapshot`` raises (e.g.
  during a bad transition between commits), the background task should
  log + continue, not exit. Commit 5 will replace the bare ``pass``
  with structured logging.
"""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional

from .models import HealthSnapshot, RegionStatus
from .region import Region
from .replication_controller import ReplicationController
from .replication_stats import ReplicationStatsTracker


class HealthMonitor:
    """Periodically snapshot cluster health for the dashboard + API."""

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
        # Failover bookkeeping (used in commit 5):
        self._unhealthy_streak: int = 0

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
        """The polling loop — never crashes, sleeps between iterations."""
        while True:
            try:
                self._last_snapshot = self.compute_snapshot()
            except Exception:
                # Never let the monitor task die. Commit 5 swaps this
                # bare ``pass`` for a structured log.
                pass
            await asyncio.sleep(self._interval)

    # ------------------------------------------------------------------
    # Snapshot construction
    # ------------------------------------------------------------------

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
        )

    def get_snapshot(self) -> HealthSnapshot:
        """Return a fresh snapshot, also caching it on ``_last_snapshot``.

        Always recomputes — see module docstring. The cached
        ``_last_snapshot`` is kept around as a debugging aid (and for
        commit 5's failover heuristics) but is *not* what gets returned
        here.
        """
        snap = self.compute_snapshot()
        self._last_snapshot = snap
        return snap
