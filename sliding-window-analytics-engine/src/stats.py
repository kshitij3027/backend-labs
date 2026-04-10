"""Incremental statistics and monotonic min/max primitives.

These data structures are the building blocks of :class:`SlidingWindow`:

* :class:`IncrementalStats` maintains count, sum, and sum-of-squares so that
  ``mean``, ``variance`` and ``std_dev`` can be derived in O(1) on every
  insert/removal. Variance is computed via the algebraic identity
  ``E[X^2] - E[X]^2``. Although that form is numerically less stable than
  Welford's algorithm, it has the key property that values can be *removed*
  as well as added — which is exactly what a sliding window needs.

* :class:`MonotonicMinMax` tracks the running minimum and maximum over a
  time-ordered stream using two monotonic deques of ``(timestamp, value)``
  pairs. Both ``add`` and ``expire_before`` are O(1) amortised.

Neither class is thread-safe; the owning :class:`SlidingWindow` is
responsible for synchronising access.
"""

from __future__ import annotations

import math
from collections import deque


class IncrementalStats:
    """O(1) incremental mean / variance / std-dev with support for removal.

    The class stores only three scalars (``count``, ``total``, ``total_sq``)
    which means memory usage is constant regardless of how many values have
    been observed. All derived quantities are computed lazily from those
    scalars on property access.
    """

    def __init__(self) -> None:
        self.count: int = 0
        self.total: float = 0.0
        self.total_sq: float = 0.0

    def add(self, value: float) -> None:
        """Fold ``value`` into the running aggregates."""
        self.count += 1
        self.total += value
        self.total_sq += value * value

    def remove(self, value: float) -> None:
        """Reverse a previous :meth:`add` for ``value``.

        The caller is responsible for only removing values that were
        previously added; removing a value that was never added leaves the
        aggregates in an inconsistent state. When ``count`` would drop to
        zero we also zero out the running sums to prevent tiny float
        residues from leaking into future windows.
        """
        self.count -= 1
        self.total -= value
        self.total_sq -= value * value
        if self.count <= 0:
            # Clamp to a clean zero state; guards against accumulating
            # floating point drift when the window oscillates around empty.
            self.count = 0
            self.total = 0.0
            self.total_sq = 0.0

    @property
    def mean(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total / self.count

    @property
    def variance(self) -> float:
        """Population variance (``ddof=0``); 0.0 for fewer than 2 samples.

        Derived from the identity ``Var(X) = E[X^2] - E[X]^2`` and clamped
        to zero to absorb the tiny negative residues that float arithmetic
        sometimes produces when all samples are (near-)equal.
        """
        if self.count < 2:
            return 0.0
        mean = self.total / self.count
        variance = (self.total_sq / self.count) - (mean * mean)
        # Clamp to handle float precision edge cases where variance
        # computes to a very small negative number (e.g. -1e-17).
        return max(0.0, variance)

    @property
    def std_dev(self) -> float:
        variance = self.variance
        if variance <= 0.0:
            return 0.0
        return math.sqrt(variance)


class MonotonicMinMax:
    """Amortised O(1) min/max over a sliding-by-timestamp stream.

    Maintains two deques of ``(timestamp, value)`` tuples:

    * ``_min_deque`` is value-increasing from head to tail, so its head is
      always the running minimum.
    * ``_max_deque`` is value-decreasing from head to tail, so its head is
      always the running maximum.

    On :meth:`add` we drop any tail entries that are now dominated by the
    new value — they can never become the min/max again because any future
    ``expire_before`` call that would remove the new entry will also remove
    them (the new entry has a later/equal timestamp by contract).

    On :meth:`expire_before` we pop from the heads of both deques until
    the oldest entry is at or past the cutoff timestamp.
    """

    def __init__(self) -> None:
        self._min_deque: deque[tuple[float, float]] = deque()
        self._max_deque: deque[tuple[float, float]] = deque()

    def add(self, timestamp: float, value: float) -> None:
        """Insert ``(timestamp, value)`` assuming non-decreasing timestamps.

        Uses ``>=`` / ``<=`` (rather than strict ``>`` / ``<``) when popping
        duplicates so that multiple entries of the same value collapse to
        a single representative, which is safe because expiring the
        "survivor" will simply expose the next distinct value.
        """
        # Maintain min-deque: pop tail entries with value >= new value,
        # since the new (later) entry will outlive them and is just as small.
        while self._min_deque and self._min_deque[-1][1] >= value:
            self._min_deque.pop()
        self._min_deque.append((timestamp, value))

        # Symmetric logic for max-deque: pop tail entries with value <= new.
        while self._max_deque and self._max_deque[-1][1] <= value:
            self._max_deque.pop()
        self._max_deque.append((timestamp, value))

    def expire_before(self, cutoff_timestamp: float) -> None:
        """Drop all entries strictly older than ``cutoff_timestamp``."""
        while self._min_deque and self._min_deque[0][0] < cutoff_timestamp:
            self._min_deque.popleft()
        while self._max_deque and self._max_deque[0][0] < cutoff_timestamp:
            self._max_deque.popleft()

    @property
    def min(self) -> float:
        """Current minimum value, or 0.0 when empty."""
        if not self._min_deque:
            return 0.0
        return self._min_deque[0][1]

    @property
    def max(self) -> float:
        """Current maximum value, or 0.0 when empty."""
        if not self._max_deque:
            return 0.0
        return self._max_deque[0][1]

    def clear(self) -> None:
        """Reset both deques to empty."""
        self._min_deque.clear()
        self._max_deque.clear()
