"""In-process simulated region for the multi-region log replication engine.

A :class:`Region` is one of the three logical regions in this project
(``us-east``, ``europe``, ``asia``). It owns:

* ``log_store`` — an in-memory dict of ``log_id → LogEntry``.
* ``vector_clock`` — its current view of cluster causality, keyed by
  region id.
* ``logical_ts`` — its own region's logical-time counter (always
  equals ``vector_clock[self.region_id]``).
* ``is_healthy`` — flipped by ``mark_offline`` / ``mark_online`` to
  simulate failure for the failover tests.

All mutating operations (``local_write``, ``receive_replication``) run
under an :class:`asyncio.Lock` so concurrent calls from
``asyncio.gather`` cannot interleave a half-updated vector clock with
its log_store write. The lock is *lazy* — created on first use — because
pytest-asyncio collects fixtures before an event loop exists, and
binding the lock at ``__init__`` time on Python 3.10+ silently associates
it with the wrong loop and raises ``RuntimeError`` on first ``acquire``.
The pattern matches the one used in
``active-passive-failover-log-processor`` (see ``src/state_machine.py``).
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List

from .conflict_resolver import resolve
from .models import LogEntry, VectorClock
from .vector_clock import increment, merge


class Region:
    """A single in-process region — primary or secondary."""

    def __init__(self, region_id: str) -> None:
        self.region_id: str = region_id
        self.log_store: Dict[str, LogEntry] = {}
        # Pre-seed our own slot at 0 so ``local_write`` cleanly increments
        # to 1 on the first call without a key lookup.
        self.vector_clock: VectorClock = {region_id: 0}
        self.logical_ts: int = 0
        self.is_healthy: bool = True
        # Lazy-init: see module docstring for why we don't bind here.
        self._lock: asyncio.Lock | None = None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_lock(self) -> asyncio.Lock:
        """Return the asyncio lock, creating it on first use.

        We deliberately delay creating the :class:`asyncio.Lock` until a
        coroutine actually awaits it, so the lock binds to the running
        event loop rather than to whatever loop happened to exist at
        construction time (often there is none, e.g. inside pytest fixture
        collection).
        """
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    # ------------------------------------------------------------------
    # Write paths
    # ------------------------------------------------------------------

    async def local_write(self, payload: Dict[str, Any]) -> LogEntry:
        """Write a new entry originating at this region.

        Increments the region's logical time, stamps the entry with the
        new vector clock, and stores it under its generated ``log_id``.

        Returns the stored entry so the caller (typically a
        ReplicationController) can fan it out to secondaries.
        """
        async with self._get_lock():
            self.logical_ts += 1
            self.vector_clock[self.region_id] = self.logical_ts
            entry = LogEntry(
                data=payload,
                region=self.region_id,
                vector_clock=dict(self.vector_clock),
                logical_ts=self.logical_ts,
            )
            self.log_store[entry.log_id] = entry
            return entry

    async def receive_replication(self, entry: LogEntry) -> LogEntry:
        """Apply a replicated entry from another region.

        Steps (per spec §2 and ``plan.md`` lines 166-173):

        1. Look up an existing entry with the same ``log_id``.
        2. If absent, the incoming entry wins by default. Otherwise call
           :func:`conflict_resolver.resolve` to pick deterministically.
        3. Store the chosen version.
        4. Merge the incoming vector clock into ours via per-key max,
           **then** increment our own region's slot by 1 — this is the
           spec-mandated "merge then increment" semantic that advances
           our logical time to reflect that we observed this event.
        5. Refresh ``logical_ts`` to track our slot in the merged clock.

        Returns the chosen :class:`LogEntry` so callers can assert which
        version landed in the store (used heavily by the unit tests).
        """
        async with self._get_lock():
            existing = self.log_store.get(entry.log_id)
            chosen = entry if existing is None else resolve(existing, entry)
            self.log_store[entry.log_id] = chosen

            # Merge then increment — spec semantic. ``merge`` returns a
            # new dict; ``increment`` returns another new dict; we then
            # rebind ``self.vector_clock`` so we never mutate either input.
            merged = merge(self.vector_clock, entry.vector_clock)
            self.vector_clock = increment(merged, self.region_id)
            self.logical_ts = self.vector_clock[self.region_id]

            return chosen

    # ------------------------------------------------------------------
    # Reads / introspection
    # ------------------------------------------------------------------

    def get_logs(self, limit: int) -> List[LogEntry]:
        """Return up to ``limit`` entries, newest first by ``created_at``.

        We sort the snapshot of values rather than maintain a side index
        because the log_store is small (test-scale) and the sort is
        O(n log n) on a few hundred entries at most.
        """
        if limit <= 0:
            return []
        all_entries = list(self.log_store.values())
        all_entries.sort(key=lambda e: e.created_at, reverse=True)
        return all_entries[:limit]

    def mark_offline(self) -> None:
        """Simulate a region failure — used by the failover tests."""
        self.is_healthy = False

    def mark_online(self) -> None:
        """Recover a previously offline region. (Failover does not auto-promote.)"""
        self.is_healthy = True

    def stats(self) -> Dict[str, Any]:
        """Return a dashboard-friendly snapshot of this region's state."""
        return {
            "region_id": self.region_id,
            "log_count": len(self.log_store),
            "vector_clock": dict(self.vector_clock),
            "logical_ts": self.logical_ts,
            "is_healthy": self.is_healthy,
        }
