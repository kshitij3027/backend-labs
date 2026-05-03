"""Replication controller — primary election + write fan-out.

The :class:`ReplicationController` is the orchestrator that ties the
three :class:`Region` objects together. Its job:

1. **Choose the primary.** Walk ``primary_preference`` in order; the
   first healthy region not in ``exclude`` becomes primary. Re-runs of
   :meth:`elect_primary` (with a different ``exclude`` set) implement
   failover when the current primary goes unhealthy.
2. **Route every write through the primary.** :meth:`write` calls
   ``primary.local_write`` to stamp the entry's vector clock + logical
   time, then fans the *same* entry out to every secondary in parallel
   via ``asyncio.gather``.
3. **Record per-region replication lag and success.** Each fan-out call
   feeds a sample into the :class:`ReplicationStatsTracker` so the
   dashboard can show ``p50/p95/p99`` and the success rate per
   secondary.

Concurrency notes:

* The election step is synchronous and pure-Python (no I/O), and the
  later HealthMonitor (commit 5) drives it from a single background
  task — there is no concurrent caller, so no lock is needed here.
* :meth:`write` uses ``return_exceptions=True`` on ``gather`` so one
  flaky secondary cannot fail the whole write. The primary write has
  already committed by the time we fan out, so a secondary error is a
  replication-level failure, not a write-level one — surfaced via the
  tracker, not the return value.
* :meth:`_replicate_to` re-raises after recording, so ``gather`` sees
  the exception and includes it in its results list. We deliberately
  *don't* swallow there because the surrounding ``return_exceptions=True``
  is the canonical place to absorb it; that keeps the per-call code
  symmetric and easier to reason about.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Set

from .models import LogEntry
from .region import Region
from .replication_stats import ReplicationStatsTracker


class ReplicationController:
    """Coordinates primary election and write replication across regions."""

    def __init__(
        self,
        regions: Dict[str, Region],
        primary_preference: List[str],
        stats: ReplicationStatsTracker,
    ) -> None:
        self._regions: Dict[str, Region] = regions
        self._primary_preference: List[str] = list(primary_preference)
        self._stats: ReplicationStatsTracker = stats
        self._primary_id: Optional[str] = None
        # Elect at construction so the controller is immediately usable
        # by the HTTP layer (commit 4) without an extra setup step.
        self.elect_primary()

    # ------------------------------------------------------------------
    # Election
    # ------------------------------------------------------------------

    def elect_primary(self, exclude: Optional[Set[str]] = None) -> str:
        """Pick a new primary region from ``primary_preference``.

        Iterates the preference list in order and returns the first
        region that is (a) not in ``exclude``, (b) actually tracked by
        this controller, and (c) currently ``is_healthy``. Stores the
        choice on ``self._primary_id`` and returns the chosen id.

        Raises:
            RuntimeError: if no preferred region passes the filter.
        """
        exclude = exclude or set()
        for r in self._primary_preference:
            if r in exclude:
                continue
            region = self._regions.get(r)
            if region is None:
                continue
            if region.is_healthy:
                self._primary_id = r
                return r
        raise RuntimeError(
            f"no healthy region available (excluded={sorted(exclude)})"
        )

    @property
    def primary_id(self) -> Optional[str]:
        """Currently-elected primary's region id (``None`` before first election)."""
        return self._primary_id

    def current_primary(self) -> Region:
        """Return the elected primary :class:`Region` object.

        Raises:
            RuntimeError: if no primary has been elected yet (shouldn't
                happen in normal operation since ``__init__`` elects
                eagerly).
        """
        if self._primary_id is None:
            raise RuntimeError("no primary elected")
        return self._regions[self._primary_id]

    def secondaries(self) -> List[Region]:
        """Return every tracked region except the current primary.

        Includes unhealthy secondaries on purpose — :meth:`_replicate_to`
        gates on ``is_healthy`` per call and records a failure either
        way. Filtering them out here would make the success-rate metric
        silently miss those failures.
        """
        return [r for rid, r in self._regions.items() if rid != self._primary_id]

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def write(self, payload: Dict[str, Any]) -> LogEntry:
        """Write to the primary, then replicate to all secondaries in parallel.

        The primary's :meth:`Region.local_write` returns the stamped
        :class:`LogEntry`; we hand the exact same entry to each secondary
        so vector clocks line up. ``asyncio.gather(..., return_exceptions=True)``
        means one secondary's failure won't propagate — replication
        errors are recorded in the tracker for observability instead.

        Returns the entry written by the primary (so the HTTP handler
        can include the stamped ``log_id`` / ``vector_clock`` in its
        response).
        """
        primary = self.current_primary()
        entry = await primary.local_write(payload)
        await asyncio.gather(
            *[self._replicate_to(s, entry) for s in self.secondaries()],
            return_exceptions=True,
        )
        return entry

    async def _replicate_to(self, secondary: Region, entry: LogEntry) -> None:
        """Replicate ``entry`` to one secondary and record the outcome.

        Records a failure (with ``lag_ms=0.0``) when the secondary is
        already unhealthy at call time — we don't bother awaiting an
        offline region. Otherwise we time the awaited
        :meth:`Region.receive_replication` call with
        :func:`time.perf_counter` (monotonic, sub-millisecond
        resolution) and record either success or failure.

        We deliberately re-raise on exception so the surrounding
        ``asyncio.gather(return_exceptions=True)`` captures it; that
        keeps the per-call code path symmetric and makes the function
        easy to unit-test in isolation.
        """
        start = time.perf_counter()
        if not secondary.is_healthy:
            self._stats.record(secondary.region_id, lag_ms=0.0, success=False)
            return
        try:
            await secondary.receive_replication(entry)
        except Exception:
            lag_ms = (time.perf_counter() - start) * 1000.0
            self._stats.record(secondary.region_id, lag_ms=lag_ms, success=False)
            raise
        lag_ms = (time.perf_counter() - start) * 1000.0
        self._stats.record(secondary.region_id, lag_ms=lag_ms, success=True)
