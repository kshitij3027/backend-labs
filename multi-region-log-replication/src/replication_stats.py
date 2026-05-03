"""Per-region replication-lag and success-rate tracker.

The :class:`ReplicationStatsTracker` keeps a rolling window of replication
lag samples (in milliseconds) for each secondary region, plus a running
total of success/failure counts. It is what powers the dashboard's
``p50/p95/p99`` numbers and the ``replication_success_rate`` field on
``RegionStatus``.

Design choices:

* **Bounded memory.** Lag samples live in a :class:`collections.deque`
  with a fixed ``maxlen`` (default 1000) so memory stays flat even after
  millions of writes. Older samples are silently dropped from the head
  as new ones arrive.
* **Successes only contribute lag samples.** A failed replication has no
  meaningful lag (we either bailed before calling the secondary or hit
  an exception mid-call), so we keep its lag out of the percentile pool
  and only bump the failure counter. This matches the "lag of healthy
  replication" semantic the dashboard reports.
* **Lazy region registration.** If a ``record`` arrives for a region we
  weren't initialised with — e.g. a future region added at runtime — we
  add an empty deque + zero counters on the fly rather than raising.
* **No lock.** ``deque`` append and ``int`` increments are atomic at the
  CPython level under the GIL; the tracker is read by a single
  background WS broadcaster and written by the controller's replication
  fan-out, both inside one event loop, so there is no concurrent
  writer.

The percentile algorithm is the simple "sort and index" approach
documented in ``plan.md`` (line 179): we sort the deque snapshot and
take the value at ``floor(p/100 * (n-1))``. This is fine for n=1000;
for production-scale telemetry one would use t-digest or HDR histograms.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Deque, Dict, List, Tuple


class ReplicationStatsTracker:
    """Per-region rolling-window lag tracker + success/failure counters."""

    def __init__(self, regions: List[str], window_size: int = 1000) -> None:
        # One deque per region — :class:`deque` with ``maxlen`` evicts oldest
        # samples automatically, giving us O(1) append and a fixed memory
        # ceiling.
        self._lag_samples: Dict[str, Deque[float]] = {
            r: deque(maxlen=window_size) for r in regions
        }
        self._success_counts: Dict[str, int] = {r: 0 for r in regions}
        self._failure_counts: Dict[str, int] = {r: 0 for r in regions}
        self._regions: List[str] = list(regions)
        self._window_size: int = window_size

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_region(self, region: str) -> None:
        """Lazily register a region the first time we see it.

        Avoids ``KeyError`` if the controller is wired with a wider region
        set than the tracker was constructed with (or vice versa during
        tests). Cheaper than enforcing a strict schema and matches the
        spirit of "the tracker observes whatever the controller sends it".
        """
        if region not in self._lag_samples:
            self._lag_samples[region] = deque(maxlen=self._window_size)
            self._success_counts[region] = 0
            self._failure_counts[region] = 0
            self._regions.append(region)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(self, region: str, lag_ms: float, success: bool) -> None:
        """Record one replication attempt.

        Args:
            region: Secondary region id the attempt targeted.
            lag_ms: Wall-clock lag in milliseconds. Ignored on failure
                (failed attempts don't have a meaningful lag to report).
            success: Whether the secondary applied the entry without
                raising.
        """
        self._ensure_region(region)
        if success:
            self._lag_samples[region].append(lag_ms)
            self._success_counts[region] += 1
        else:
            self._failure_counts[region] += 1

    # ------------------------------------------------------------------
    # Aggregations
    # ------------------------------------------------------------------

    def percentiles(self, region: str) -> Tuple[float, float, float]:
        """Return ``(p50, p95, p99)`` for the rolling lag window.

        Falls back to ``(0.0, 0.0, 0.0)`` when no samples have been
        recorded yet — the dashboard prefers a numeric zero to a missing
        field for layout reasons.

        The index for percentile ``p`` is ``floor(p/100 * (n-1))`` where
        ``n`` is the current sample count. With ``n=10`` and ``p=50`` the
        index is ``floor(0.5 * 9) = 4`` — the 5th sample of a sorted
        sequence ``[10, 20, ..., 100]`` is ``50``, which matches the unit
        test's expectation.
        """
        samples = self._lag_samples.get(region)
        if not samples:
            return (0.0, 0.0, 0.0)

        sorted_samples = sorted(samples)
        n = len(sorted_samples)

        def _at(p: float) -> float:
            # ``floor(p/100 * (n-1))`` — the algorithm fixed in plan.md.
            idx = int(math.floor((p / 100.0) * (n - 1)))
            return float(sorted_samples[idx])

        return (_at(50.0), _at(95.0), _at(99.0))

    def success_rate(self, region: str) -> float:
        """Return ``successes / (successes + failures)``.

        Returns ``0.0`` when both counters are zero (no attempts yet) so
        the caller never divides by zero. Note this is asymmetric with
        "100% healthy"; a region with zero attempts is reported as 0.0,
        not 1.0 — the dashboard distinguishes the two via ``sample_count``.
        """
        s = self._success_counts.get(region, 0)
        f = self._failure_counts.get(region, 0)
        total = s + f
        if total == 0:
            return 0.0
        return s / total

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        """Return a per-region dict of all derived stats.

        Shape::

            {
              "us-east": {"p50": 12.3, "p95": 45.6, "p99": 89.0,
                          "success_rate": 0.98, "sample_count": 250},
              ...
            }

        ``sample_count`` is the **current** size of the lag deque, so it
        tops out at ``window_size`` (1000 by default) regardless of how
        many writes the region has serviced overall.
        """
        out: Dict[str, Dict[str, float]] = {}
        for region in self._regions:
            p50, p95, p99 = self.percentiles(region)
            out[region] = {
                "p50": p50,
                "p95": p95,
                "p99": p99,
                "success_rate": self.success_rate(region),
                "sample_count": len(self._lag_samples.get(region, ())),
            }
        return out
