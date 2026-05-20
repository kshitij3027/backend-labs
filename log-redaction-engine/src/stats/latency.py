"""Reservoir-sampled latency histogram.

Stores up to ``reservoir_size`` latency observations and computes
p50/p95/p99 percentiles from the reservoir on demand. Reservoir
sampling (Vitter's "Algorithm R") gives a uniformly random sample of
the observation stream in bounded memory — once the reservoir is full,
each new observation has probability ``reservoir_size / total_count``
of replacing a random existing entry.

Why reservoir sampling?
-----------------------
A naive "keep all samples" approach grows unboundedly. A naive
"sliding window of N most recent samples" is biased toward the present
and loses long-tail observations entirely. Reservoir sampling gives
unbiased percentiles at constant memory.

Thread safety
-------------
A single coarse lock guards the reservoir + counter. The reservoir is
mutated only inside the lock; ``random.randint`` is invoked under the
lock to keep the read-modify-write of ``self._count`` and
``self._reservoir`` atomic.
"""
from __future__ import annotations

import random
import threading


class LatencyHistogram:
    """Reservoir-sampled latency histogram with percentile snapshots.

    Parameters
    ----------
    reservoir_size : int, default 1024
        Maximum number of samples retained. Memory usage is O(N) so
        1024 floats ≈ 8 KB per histogram — cheap to keep in RAM.

    Notes
    -----
    Percentiles are computed from a sorted copy of the reservoir on
    every :meth:`snapshot` call. For 1024 samples this is < 100µs;
    the dashboard pulls stats at most once per second so the cost is
    negligible.
    """

    def __init__(self, reservoir_size: int = 1024) -> None:
        self._reservoir: list[float] = []
        self._reservoir_size = reservoir_size
        # Total observation count — required for reservoir sampling's
        # probability calculation (``idx = randint(0, count-1)``).
        # Continues incrementing past reservoir_size, by design.
        self._count = 0
        self._lock = threading.Lock()

    def record(self, latency_ms: float) -> None:
        """Add a single latency observation to the reservoir.

        Algorithm (Vitter's "Algorithm R"):

        1. Increment the total observation count.
        2. If the reservoir is not full, append directly.
        3. Otherwise, pick a uniform random index in
           ``[0, count - 1]``. If the index is within the reservoir,
           replace the entry at that index; otherwise drop the sample.

        The probability of any sample surviving in the reservoir
        approaches ``reservoir_size / count``, which is the uniformly
        random property reservoir sampling guarantees.
        """
        with self._lock:
            # Increment first so ``self._count - 1`` is the upper
            # bound of the index range (inclusive). This matches the
            # standard formulation of Algorithm R.
            self._count += 1
            if len(self._reservoir) < self._reservoir_size:
                # Reservoir still has room — append unconditionally.
                self._reservoir.append(latency_ms)
            else:
                # Reservoir full. ``randint(0, count-1)`` is uniform
                # in [0, count-1]; the chance that it lands within the
                # reservoir is ``reservoir_size / count``, which is the
                # correct survival probability.
                idx = random.randint(0, self._count - 1)
                if idx < self._reservoir_size:
                    self._reservoir[idx] = latency_ms

    def snapshot(self) -> dict:
        """Return a dict with count + mean + p50 / p95 / p99.

        Returns
        -------
        dict
            Keys: ``count``, ``mean_ms``, ``p50_ms``, ``p95_ms``,
            ``p99_ms``. All values are floats except ``count`` which
            is the integer total observation count (not the reservoir
            size). An empty histogram returns all zeros.

        Notes
        -----
        The dict shape is what the C8 dashboard's HTMX poll expects.
        Sorting the reservoir on each call is O(N log N) with N ≤
        1024 — well under 1 ms in practice.
        """
        with self._lock:
            if len(self._reservoir) == 0:
                # Defensive: no samples yet -> all zeros. Avoids a
                # division-by-zero in the mean calculation and gives
                # the dashboard a stable shape on a cold start.
                return {
                    "count": 0,
                    "mean_ms": 0.0,
                    "p50_ms": 0.0,
                    "p95_ms": 0.0,
                    "p99_ms": 0.0,
                }
            # Sort a copy so the live reservoir isn't disturbed. Sorting
            # under the lock keeps the snapshot consistent with the
            # reservoir state at lock-acquisition time.
            sorted_values = sorted(self._reservoir)
            mean = sum(sorted_values) / len(sorted_values)
            return {
                "count": self._count,
                "mean_ms": mean,
                "p50_ms": self._percentile(sorted_values, 50),
                "p95_ms": self._percentile(sorted_values, 95),
                "p99_ms": self._percentile(sorted_values, 99),
            }

    @staticmethod
    def _percentile(sorted_values: list[float], p: int) -> float:
        """Return the ``p``th percentile of ``sorted_values``.

        Uses the simple nearest-rank method: ``idx = floor(len * p /
        100)``. Clamped into ``[0, len - 1]`` so a 100th percentile on
        a non-empty list returns the maximum, and a 0th percentile
        returns the minimum.

        Notes
        -----
        Nearest-rank is the simplest defensible percentile algorithm
        and matches what a "quick eyeballing" reader expects. Other
        methods (linear interpolation, Hyndman-Fan) trade simplicity
        for finer-grained answers on tiny samples — not worth it for
        a 1024-element reservoir.
        """
        # ``int()`` truncates toward zero, which matches the standard
        # nearest-rank formulation. ``max(0, min(...))`` clamps so the
        # edge cases (p=0, p=100) don't fall off either end.
        idx = max(0, min(len(sorted_values) - 1, int(len(sorted_values) * p / 100)))
        return sorted_values[idx]
