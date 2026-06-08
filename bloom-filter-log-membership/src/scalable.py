"""Scalable Bloom filter — adaptive sizing without a known capacity up front.

A plain :class:`~src.bloom.BloomFilter` has one rigid precondition: you must
know ``expected_items`` at construction time. Undershoot and the filter
saturates (false positives blow past the target); overshoot and the memory
win evaporates. Log streams are exactly the workload where the right number
is unknowable in advance — that gap is what this class closes (Extended B,
"adaptive sizing").

The design is Almeida, Baquero, Preguiça & Hutchison, "Scalable Bloom
Filters" (Information Processing Letters, 2007): a *series* of plain Bloom
filter "slices".

* All inserts land in the newest ("active") slice. The moment it reaches its
  design capacity, a fresh slice is appended with **geometrically larger
  capacity** (factor ``growth``, s=2 here) and a **geometrically tighter
  error budget** (ratio ``tightening``, r=0.85 here); the next insert lands
  there.
* A query ORs ``might_contain`` across every slice. Every admitted key lives
  entirely inside exactly one slice and bits are never cleared, so the
  zero-false-negative guarantee of the underlying filters survives growth
  unchanged.

The error budget (the part that is easy to get wrong)
-----------------------------------------------------
Each slice is an independent chance to answer a false "yes", so the compound
false-positive probability is ``1 - prod(1 - fp_i)`` (≈ ``sum(fp_i)`` for
small rates) over *all slices ever created*. The budget therefore has to be
split across the whole, unbounded series — not granted per slice. Slice
``i`` gets:

    capacity_i = initial_capacity * growth ** i
    fp_i       = target_fp_rate * (1 - tightening) * tightening ** i

The fp series is geometric, so it sums to exactly the advertised target no
matter how far the filter grows::

    sum_{i>=0} fp_i = target * (1 - r) * (1 / (1 - r)) = target    (r = tightening)

Contrast the naive variant that grants slice 0 the full target and merely
multiplies by r afterwards (``fp_i = target * r**i``): that series converges
to ``target / (1 - r)`` — at r=0.85 a compound rate of ~6.7x the advertised
target. The ``(1 - r)`` down-payment on slice 0 is what turns "compound FP
never exceeds the target" into a theorem instead of a hope, at the price of
slice 0 being sized ~1.5x larger than a fixed filter at the same target
(tighter fp costs more bits per element).

Why ``add`` deduplicates first
------------------------------
``add`` checks ``might_contain`` across all slices *before* inserting and
returns ``False`` without touching a single bit when the key already reads
as present. Two reasons:

* **Growth correctness.** Slices grow when the active slice's distinct-admit
  count reaches its capacity. Log workloads are duplicate-heavy (replays,
  retries, repeated log keys — the normal case for this project); without
  the pre-check, every duplicate would be re-inserted into the newest slice,
  inflating its count and driving real memory growth that encodes zero new
  information.
* **Count semantics.** ``count`` stays "distinct keys admitted", matching
  the single-filter semantics that metrics (C5) and ``/stats`` (C8) build on.

The cost is the paper's own trade: a brand-new key that happens to be a
compound false positive is silently treated as a duplicate and skipped
(probability ≤ ``target_fp_rate`` by the budget above). Queries are
unaffected — such a key already answers ``True`` everywhere it matters — so
the zero-false-negative contract holds for every key ever passed to ``add``.

Why each slice hashes with a different seed
-------------------------------------------
Slice ``i`` seeds murmur3 with ``seed + i``. With one shared seed, every
slice would derive the *same* (h1, h2) pair for a given key — only reduced
modulo a different m — so slice answers would be correlated: a key that
false-positives in one slice would be unusually likely to false-positive in
the next. The compound bound ``1 - prod(1 - fp_i)`` (and the whole point of
paying for tightening) assumes independent slices; distinct seeds give each
slice an effectively independent hash family, so the budget buys what it
pays for.

Thread safety
-------------
Deliberately NOT this class's job — the same stance as ``BloomFilter``. The
per-log-type ``FilterManager`` (C7) owns one ``threading.Lock`` per named
filter and serializes all access through it; keeping this class lock-free
keeps it trivially testable and avoids double-locking under the manager.

Persistence: the ``SBF1`` container in :mod:`src.persistence` snapshots the
series parameters plus one ``BLM1`` blob per slice.
"""
from __future__ import annotations

import math

from src.bloom import DEFAULT_SEED, BloomFilter


class ScalableBloomFilter:
    """Almeida et al. scalable Bloom filter over UTF-8 string keys.

    A growable series of :class:`~src.bloom.BloomFilter` slices with
    geometric capacity growth and a geometric false-positive budget whose
    infinite sum equals ``target_fp_rate`` exactly — the compound FP rate
    stays at or below the target no matter how many slices growth appends
    (see the module docstring for the math). Not thread-safe by design; the
    owning manager (C7) provides locking.
    """

    def __init__(
        self,
        initial_capacity: int,
        target_fp_rate: float,
        growth: int = 2,
        tightening: float = 0.85,
        seed: int = DEFAULT_SEED,
    ) -> None:
        if initial_capacity < 1:
            raise ValueError(
                f"initial_capacity must be >= 1, got {initial_capacity!r}"
            )
        if not 0.0 < target_fp_rate < 1.0:
            raise ValueError(
                "target_fp_rate must be strictly between 0 and 1, "
                f"got {target_fp_rate!r}"
            )
        if growth < 2:
            raise ValueError(
                f"growth must be >= 2 (no growth below doubling), got {growth!r}"
            )
        if not 0.0 < tightening < 1.0:
            raise ValueError(
                f"tightening must be strictly between 0 and 1, got {tightening!r}"
            )

        self._initial_capacity = initial_capacity
        self._target_fp_rate = target_fp_rate
        self._growth = growth
        self._tightening = tightening
        self._seed = seed
        # Slice 0 exists from birth: queries on an empty filter need
        # somewhere to look and the first add needs somewhere to land, so
        # there is never a "no active slice" state to special-case.
        self._slices: list[BloomFilter] = [self._new_slice(0)]

    # ------------------------------------------------------------------ #
    # slice construction                                                 #
    # ------------------------------------------------------------------ #

    def _new_slice(self, index: int) -> BloomFilter:
        """Build slice ``index`` of the series.

        * ``capacity_i = initial_capacity * growth**i`` — geometric growth
          keeps the slice count (and therefore per-query work, which touches
          every slice) logarithmic in the total number of admitted keys.
        * ``fp_i = target * (1 - tightening) * tightening**i`` — the
          paper-faithful geometric budget that sums to exactly the target
          (module docstring).
        * ``seed + index`` — an independent murmur3 hash family per slice,
          so per-slice false positives are uncorrelated and the compound
          product bound actually applies (module docstring).
        """
        return BloomFilter(
            expected_items=self._initial_capacity * self._growth**index,
            fp_rate=self._target_fp_rate
            * (1.0 - self._tightening)
            * self._tightening**index,
            seed=self._seed + index,
        )

    # ------------------------------------------------------------------ #
    # core operations                                                    #
    # ------------------------------------------------------------------ #

    def add(self, item: str) -> bool:
        """Admit ``item`` into the active slice; return True if it was new.

        Dedup-first: if any slice already answers ``might_contain`` (a real
        prior admit *or* a compound false positive — indistinguishable by
        construction), return False without touching any bits, so duplicate
        streams can never drive slice growth. Otherwise insert into the
        newest slice; since the dedup miss proves at least one of the key's
        bits there is 0, the underlying add always flips a bit and returns
        True.

        Growth happens *after* the insert that fills the active slice: when
        its distinct-admit count reaches its design capacity, a fresh slice
        with the next capacity/budget/seed in the series is appended, and
        the next admit lands there.
        """
        if self.might_contain(item):
            return False
        active = self._slices[-1]
        added = active.add(item)
        if active.count >= active.expected_items:
            self._slices.append(self._new_slice(len(self._slices)))
        return added

    def might_contain(self, item: str) -> bool:
        """Return False only if ``item`` was definitely never admitted.

        OR across all slices: every admitted key lives entirely inside
        exactly one slice (the one that was active at admit time) and bits
        are never cleared, so a False here is a proof of absence — the
        zero-false-negative guarantee survives growth. Any single slice
        answering True makes the whole filter answer True, which is exactly
        why the per-slice budgets are tightened (module docstring).
        """
        return any(s.might_contain(item) for s in self._slices)

    # ------------------------------------------------------------------ #
    # introspection                                                      #
    # ------------------------------------------------------------------ #

    @property
    def initial_capacity(self) -> int:
        """Design capacity of slice 0 — the series' n0 (persists in SBF1)."""
        return self._initial_capacity

    @property
    def target_fp_rate(self) -> float:
        """Overall compound FP target the slice budget series sums to."""
        return self._target_fp_rate

    @property
    def growth(self) -> int:
        """Capacity multiplier between consecutive slices (paper's s)."""
        return self._growth

    @property
    def tightening(self) -> float:
        """Error-budget ratio between consecutive slices (paper's r)."""
        return self._tightening

    @property
    def seed(self) -> int:
        """Base hash seed; slice i hashes with ``seed + i``."""
        return self._seed

    @property
    def count(self) -> int:
        """Total distinct keys admitted across all slices."""
        return sum(s.count for s in self._slices)

    @property
    def capacity(self) -> int:
        """Total design capacity across all current slices."""
        return sum(s.expected_items for s in self._slices)

    @property
    def slice_count(self) -> int:
        """Number of slices in the series so far (≥ 1)."""
        return len(self._slices)

    @property
    def memory_bytes(self) -> int:
        """Total raw bitset payload across all slices, in bytes."""
        return sum(s.memory_bytes for s in self._slices)

    @property
    def slices(self) -> tuple[BloomFilter, ...]:
        """Read-only view of the slice series, oldest first.

        A tuple snapshot: callers (persistence, stats, tests) can iterate
        and introspect but cannot reorder or replace slices behind the
        filter's back. The newest slice — the only one that accepts inserts
        — is ``slices[-1]``.
        """
        return tuple(self._slices)

    @property
    def compound_estimated_fp(self) -> float:
        """Live compound FP estimate: ``1 - prod(1 - est_i)`` over slices.

        Each slice contributes its fill-based ``estimated_fp_rate``; a query
        false-positives if *any* slice does, hence the union/product form.
        By the budget series this stays at or below ``target_fp_rate`` even
        with every slice filled to design capacity — this is the operational
        gauge the two-tier pipeline's fallback threshold (C10) watches.
        """
        return 1.0 - math.prod(
            1.0 - s.estimated_fp_rate for s in self._slices
        )

    def stats(self) -> dict:
        """Return aggregate gauges plus per-slice detail (JSON-friendly).

        The nested ``slices`` list reuses :meth:`BloomFilter.stats` verbatim
        so /stats (C8) and the dashboard (C12) can render the series without
        knowing slice internals.
        """
        return {
            "target_fp_rate": self._target_fp_rate,
            "compound_estimated_fp": self.compound_estimated_fp,
            "count": self.count,
            "capacity": self.capacity,
            "slice_count": self.slice_count,
            "memory_bytes": self.memory_bytes,
            "growth": self._growth,
            "tightening": self._tightening,
            "slices": [s.stats() for s in self._slices],
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"ScalableBloomFilter(n0={self._initial_capacity}, "
            f"p={self._target_fp_rate}, s={self._growth}, "
            f"r={self._tightening}, slices={self.slice_count}, "
            f"count={self.count})"
        )
