"""Core Bloom filter — the probabilistic heart of the membership service.

A Bloom filter answers "have we seen this key before?" using a fixed-size bit
array instead of storing the keys themselves. The trade is asymmetric and is
the whole point of this project:

* **"No" is always right (zero false negatives).** ``add`` only ever sets bits
  0→1, never clears them. So if *any* of a key's k probe positions holds a 0,
  that key was definitely never added — there is no sequence of inserts that
  could have left it 0. ``might_contain`` returning ``False`` is a proof.
* **"Yes" is only probably right (bounded false positives).** All k positions
  being 1 may just mean other keys happened to set those same bits. The filter
  is sized so that probability stays at or below a target ``fp_rate``.

Sizing math (Bloom 1970, standard results)
------------------------------------------
For ``n`` expected items at target false-positive probability ``p``:

    m = ceil( -(n * ln p) / (ln 2)^2 )      bits in the array
    k = round( (m / n) * ln 2 )             number of hash probes per key

Bits-per-element depends only on ``p`` — this is where the memory win over a
hash set of full keys comes from (a 64-byte log key costs 512 bits in a set,
~10 bits here). The three spec configs land at:

    p = 0.01   → ~9.6  bits/element, k = 7   (error_logs,    1M keys ≈ 1.14 MiB)
    p = 0.05   → ~6.2  bits/element, k = 4   (access_logs,   5M keys ≈ 3.7  MiB)
    p = 0.001  → ~14.4 bits/element, k = 10  (security_logs, 100K keys ≈ 175 KiB)

``optimal_m`` additionally rounds m **up to the next multiple of 8**: the
persistence layer (C4) serializes the backing ``bitarray`` with
``tobytes()``/``frombytes()``, which pad to whole bytes — byte-aligned m makes
that roundtrip exact instead of "exact modulo trailing pad bits". Rounding up
(never down) can only lower the real FP rate below target.

Kirsch–Mitzenmacher double hashing
----------------------------------
Naively, k probes need k independent hash functions. Kirsch & Mitzenmacher
("Less Hashing, Same Performance: Building a Better Bloom Filter", 2006)
proved that two independent hashes h1, h2 combined as

    index_i = (h1 + i * h2) mod m        for i = 0 .. k-1

give the same asymptotic false-positive rate as k truly independent hashes.
Even better, ONE ``mmh3.hash128`` call already yields 128 independent-ish
bits, so we split a single digest into two 64-bit halves: ``h1`` = low 64
bits, ``h2`` = high 64 bits **forced odd** (``| 1``) so the stride is never 0
(an h2 of 0 would degenerate all k probes onto the same bit) and never shares
a factor 2 with our even, byte-aligned m. Net cost per add/query: exactly one
hash call plus k modular index computations.

Live false-positive estimates
-----------------------------
* ``estimated_fp_rate`` — ``(bits_set / m) ** k``: the chance a never-added
  key finds all k probes set, computed from the *actual* observed fill. This
  is the number the two-tier pipeline (C10) watches for its fallback trigger.
* ``theoretical_fp_rate`` — ``(1 - e^(-k * count / m)) ** k``: the textbook
  expectation at the current insert count; useful as a sanity cross-check
  against the live estimate.

Thread safety
-------------
Deliberately NOT this class's job. ``BloomFilter`` is a plain single-threaded
data structure; the per-log-type ``FilterManager`` (C7) owns one
``threading.Lock`` per named filter and serializes all access through it.
Keeping the locking out of the hot loop here keeps the class trivially
testable and avoids paying for a lock in single-owner contexts.
"""
from __future__ import annotations

import math

import mmh3
from bitarray import bitarray

#: Low-64-bit mask used to split the 128-bit murmur digest into h1 / h2.
_MASK64 = (1 << 64) - 1

#: Default hash seed ("SEED BLOC"). Fixed so filters are reproducible across
#: runs and across processes — persistence (C4) stores the seed alongside the
#: bits, and a restored filter must probe the exact same positions.
DEFAULT_SEED = 0x5EEDB10C


def optimal_m(n: int, p: float) -> int:
    """Return the optimal bit-array size for ``n`` items at FP rate ``p``.

    Computes ``ceil(-(n * ln p) / (ln 2)^2)`` and then rounds UP to the next
    multiple of 8 so the array is byte-aligned (exact ``tobytes`` /
    ``frombytes`` persistence roundtrips — see module docstring). A
    module-level pure function so the sizing math is testable on its own.
    """
    raw = math.ceil(-(n * math.log(p)) / (math.log(2) ** 2))
    return ((raw + 7) // 8) * 8


def optimal_k(n: int, m: int) -> int:
    """Return the optimal number of hash probes for ``n`` items in ``m`` bits.

    ``round((m / n) * ln 2)``, floored at 1 — k=0 would make every query a
    vacuous "yes". At the optimum each probe roughly halves the residual FP
    probability, which is why smaller targets cost more probes (0.01 → 7,
    0.001 → 10).
    """
    return max(1, round((m / n) * math.log(2)))


class BloomFilter:
    """Classic fixed-size Bloom filter over UTF-8 string keys.

    Sized from ``expected_items`` / ``fp_rate`` via :func:`optimal_m` and
    :func:`optimal_k`; hashes with seeded murmur3 double hashing (one
    ``mmh3.hash128`` call per operation). See the module docstring for the
    math and the zero-false-negative guarantee. Not thread-safe by design —
    the owning manager (C7) provides locking.
    """

    def __init__(
        self,
        expected_items: int,
        fp_rate: float,
        seed: int = DEFAULT_SEED,
    ) -> None:
        if expected_items < 1:
            raise ValueError(
                f"expected_items must be >= 1, got {expected_items!r}"
            )
        if not 0.0 < fp_rate < 1.0:
            raise ValueError(
                f"fp_rate must be strictly between 0 and 1, got {fp_rate!r}"
            )

        self._expected_items = expected_items
        self._fp_rate = fp_rate
        self._seed = seed
        self._m = optimal_m(expected_items, fp_rate)
        self._k = optimal_k(expected_items, self._m)
        # The backing store: m zeroed bits. bitarray is a C extension, so
        # this really is m/8 bytes of payload — the memory win is real.
        self._bits = bitarray(self._m)
        self._bits.setall(0)
        # Distinct-ish insert count (see add()); drives theoretical_fp_rate.
        self._count = 0
        # Cached popcount of _bits, maintained incrementally by add() — kept
        # so estimated_fp_rate stays O(1) on hot lookup paths. bitarray's
        # count() is C-fast but O(m): at access_logs scale (~31 Mbit) that
        # is ~ms-scale, unacceptable per-query.
        self._bits_set = 0

    # ------------------------------------------------------------------ #
    # hashing                                                            #
    # ------------------------------------------------------------------ #

    def _indexes(self, item: str) -> list[int]:
        """Return the k bit positions probed for ``item``.

        Kirsch–Mitzenmacher: one 128-bit murmur digest split into h1 (low 64
        bits) and h2 (high 64 bits, forced odd so the stride is never zero
        and never collapses against the even, byte-aligned m), expanded as
        ``(h1 + i*h2) mod m`` for i in 0..k-1.
        """
        digest = mmh3.hash128(item.encode("utf-8"), self._seed, signed=False)
        h1 = digest & _MASK64
        h2 = (digest >> 64) | 1
        m = self._m
        return [(h1 + i * h2) % m for i in range(self._k)]

    # ------------------------------------------------------------------ #
    # core operations                                                    #
    # ------------------------------------------------------------------ #

    def add(self, item: str) -> bool:
        """Set ``item``'s k bits; return True only if the item was new.

        "New" means at least one of its bits flipped 0→1 — a re-add of an
        existing key touches only already-set bits and returns False.
        ``_count`` increments only for new items, so duplicate-heavy streams
        do not inflate the count (the scalable filter in C6 relies on this
        to avoid duplicate-driven slice growth).
        """
        bits = self._bits
        flipped = 0  # bits this call turned 0→1
        for index in self._indexes(item):
            if not bits[index]:
                bits[index] = 1
                flipped += 1
        new = flipped > 0
        if new:
            self._count += 1
            self._bits_set += flipped  # maintain the O(1) popcount cache
        return new

    def might_contain(self, item: str) -> bool:
        """Return False if ``item`` was definitely never added, else True.

        A single 0 among the k probed bits is proof of absence (bits are
        never cleared) — that is the zero-false-negative guarantee. All-1s
        means "probably added": with probability ~``fp_rate`` it is other
        keys' bits lining up.
        """
        bits = self._bits
        return all(bits[index] for index in self._indexes(item))

    # ------------------------------------------------------------------ #
    # introspection                                                      #
    # ------------------------------------------------------------------ #

    @property
    def m(self) -> int:
        """Size of the bit array in bits (byte-aligned, multiple of 8)."""
        return self._m

    @property
    def k(self) -> int:
        """Number of hash probes per key."""
        return self._k

    @property
    def count(self) -> int:
        """Number of distinct-ish items added (duplicates excluded).

        "ish" because a brand-new key whose k bits all collide with already
        set bits is indistinguishable from a duplicate — at the target fill
        that happens with probability ~``fp_rate``, so the count reads at
        most a hair low. Good enough to drive the theoretical FP estimate.
        """
        return self._count

    @property
    def bits_set(self) -> int:
        """Number of 1-bits currently in the array (cached popcount).

        Served from a counter that ``add`` maintains incrementally — bits
        only ever flip 0→1, so summing each call's flips is exact. Cached to
        keep ``estimated_fp_rate`` O(1) on hot lookup paths: a real
        ``bitarray.count()`` is C-fast but O(m), and at 31 Mbit
        (access_logs) it is ~ms-scale — unacceptable per-query.
        """
        return self._bits_set

    @property
    def fill_ratio(self) -> float:
        """Fraction of bits set, 0.0..1.0 — the filter's "fullness" gauge."""
        return self.bits_set / self._m

    @property
    def memory_bytes(self) -> int:
        """Raw bitset payload size in bytes (exact: m is byte-aligned).

        This is what ``tobytes()`` will serialize in C4 and the number quoted
        in memory-vs-hash-set comparisons; Python object overhead excluded.
        """
        return self._m // 8

    @property
    def expected_items(self) -> int:
        """Capacity n the filter was sized for."""
        return self._expected_items

    @property
    def fp_rate(self) -> float:
        """Target (design-time) false-positive rate p."""
        return self._fp_rate

    @property
    def seed(self) -> int:
        """Hash seed — must travel with the bits for persistence (C4)."""
        return self._seed

    @property
    def bits(self) -> bitarray:
        """The live backing bitarray (not a copy) — for the persistence layer.

        C4 serializes this directly with ``tobytes()`` and restores with
        ``frombytes()``; handing out the real object avoids copying ~MBs on
        every snapshot. Callers must not mutate it.
        """
        return self._bits

    @property
    def estimated_fp_rate(self) -> float:
        """Live FP estimate from the *actual* fill: ``(bits_set / m) ** k``.

        The probability that a never-added key finds all k of its probe bits
        already set, given the observed bit density. 0.0 while empty. This is
        the operational health number (C10's fallback threshold watches it).
        """
        return (self.bits_set / self._m) ** self._k

    @property
    def theoretical_fp_rate(self) -> float:
        """Textbook FP expectation at the current count.

        ``(1 - e^(-k * count / m)) ** k`` — what the estimate *should* read
        after ``count`` distinct inserts; reaches ~``fp_rate`` as the count
        reaches ``expected_items``.
        """
        return (1.0 - math.exp(-self._k * self._count / self._m)) ** self._k

    def stats(self) -> dict:
        """Return all gauges in one plain dict (JSON-friendly, for /stats)."""
        return {
            "m_bits": self._m,
            "k_hashes": self._k,
            "count": self._count,
            "bits_set": self.bits_set,
            "fill_ratio": self.fill_ratio,
            "memory_bytes": self.memory_bytes,
            "expected_items": self._expected_items,
            "target_fp_rate": self._fp_rate,
            "estimated_fp_rate": self.estimated_fp_rate,
            "theoretical_fp_rate": self.theoretical_fp_rate,
            "seed": self._seed,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"BloomFilter(n={self._expected_items}, p={self._fp_rate}, "
            f"m={self._m}, k={self._k}, count={self._count})"
        )
