"""Unit tests for src.bloom — sizing math, hashing, and the core guarantees.

Everything here is deterministic (fixed default seed, no randomness, no
network) and fast: the largest filter built is ~3.7 MiB of bits and the
heaviest loops are 10k murmur hashes — milliseconds in C.
"""
from __future__ import annotations

import math

import pytest

from src.bloom import BloomFilter, optimal_k, optimal_m


def raw_m(n: int, p: float) -> int:
    """The textbook (pre-byte-alignment) optimal bit count."""
    return math.ceil(-(n * math.log(p)) / (math.log(2) ** 2))


class TestSizing:
    """m/k formulas for the three per-log-type spec configs."""

    def test_error_logs_config_1m_p01(self) -> None:
        """n=1M, p=0.01 → m=9_585_064 (9_585_059 byte-rounded), k=7, ~1.14 MiB."""
        bf = BloomFilter(expected_items=1_000_000, fp_rate=0.01)
        assert raw_m(1_000_000, 0.01) == 9_585_059
        assert bf.m == 9_585_064  # next multiple of 8 above the raw ceil
        assert bf.m % 8 == 0
        assert bf.k == 7
        assert bf.memory_bytes == bf.m // 8 == 1_198_133
        # ~9.6 bits/element ⇒ just over a megabyte for a million keys.
        assert 1_100_000 < bf.memory_bytes < 1_200_000

    def test_access_logs_config_5m_p05(self) -> None:
        """n=5M, p=0.05 → k=4; m byte-aligned within 8 bits of the formula."""
        bf = BloomFilter(expected_items=5_000_000, fp_rate=0.05)
        raw = raw_m(5_000_000, 0.05)
        assert raw <= bf.m < raw + 8
        assert bf.m % 8 == 0
        assert bf.k == 4
        assert bf.memory_bytes == bf.m // 8

    def test_security_logs_config_100k_p001(self) -> None:
        """n=100K, p=0.001 → k=10; m byte-aligned within 8 bits of the formula."""
        bf = BloomFilter(expected_items=100_000, fp_rate=0.001)
        raw = raw_m(100_000, 0.001)
        assert raw <= bf.m < raw + 8
        assert bf.m % 8 == 0
        assert bf.k == 10
        assert bf.memory_bytes == bf.m // 8

    def test_module_level_sizing_functions_agree_with_filter(self) -> None:
        """The pure functions are exactly what __init__ wires in."""
        bf = BloomFilter(expected_items=10_000, fp_rate=0.01)
        assert bf.m == optimal_m(10_000, 0.01)
        assert bf.k == optimal_k(10_000, bf.m)


class TestValidation:
    """Constructor argument validation."""

    def test_zero_expected_items_rejected(self) -> None:
        with pytest.raises(ValueError):
            BloomFilter(expected_items=0, fp_rate=0.01)

    @pytest.mark.parametrize("bad_fp_rate", [0.0, 1.0, -0.1, 1.5])
    def test_out_of_range_fp_rate_rejected(self, bad_fp_rate: float) -> None:
        """fp_rate must be strictly inside (0, 1)."""
        with pytest.raises(ValueError):
            BloomFilter(expected_items=1_000, fp_rate=bad_fp_rate)


class TestMembership:
    """The two sides of the probabilistic contract."""

    def test_zero_false_negatives_on_10k_keys(self) -> None:
        """THE core guarantee: every added key must answer True, no exceptions."""
        bf = BloomFilter(expected_items=10_000, fp_rate=0.01)
        keys = [f"key-{i}" for i in range(10_000)]
        for key in keys:
            bf.add(key)
        missing = [key for key in keys if not bf.might_contain(key)]
        assert missing == []

    def test_empty_filter_answers_definitely_absent(self) -> None:
        """With no bits set, any query is a provable miss; gauges read zero."""
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        assert bf.might_contain("anything") is False
        assert bf.might_contain("") is False
        assert bf.count == 0
        assert bf.bits_set == 0
        assert bf.fill_ratio == 0.0
        assert bf.estimated_fp_rate == 0.0
        assert bf.theoretical_fp_rate == 0.0

    def test_observed_fp_rate_bounded_on_disjoint_keys(self) -> None:
        """At design capacity, observed FPs stay under 2x the 1% target.

        10k inserted keys, 10k probes from a disjoint key space; with the
        fixed seed the outcome is deterministic, and 2x leaves comfortable
        statistical headroom over the expected ~1%.
        """
        bf = BloomFilter(expected_items=10_000, fp_rate=0.01)
        for i in range(10_000):
            bf.add(f"key-{i}")
        false_positives = sum(
            bf.might_contain(f"absent-{i}") for i in range(10_000)
        )
        assert false_positives / 10_000 < 0.02

    def test_stats_keys_and_values(self) -> None:
        """stats() exposes every gauge under its documented key."""
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        bf.add("one")
        stats = bf.stats()
        assert set(stats) == {
            "m_bits",
            "k_hashes",
            "count",
            "bits_set",
            "fill_ratio",
            "memory_bytes",
            "expected_items",
            "target_fp_rate",
            "estimated_fp_rate",
            "theoretical_fp_rate",
            "seed",
        }
        assert stats["m_bits"] == bf.m
        assert stats["k_hashes"] == bf.k
        assert stats["count"] == bf.count == 1
        assert stats["bits_set"] == bf.bits_set
        assert stats["fill_ratio"] == bf.fill_ratio
        assert stats["memory_bytes"] == bf.memory_bytes
        assert stats["expected_items"] == 1_000
        assert stats["target_fp_rate"] == 0.01
        assert stats["estimated_fp_rate"] == bf.estimated_fp_rate
        assert stats["theoretical_fp_rate"] == bf.theoretical_fp_rate
        assert stats["seed"] == bf.seed


class TestCounting:
    """add() return value and count semantics."""

    def test_duplicate_add_is_a_noop(self) -> None:
        """First add of a key is new (True); the re-add is not (False)."""
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        assert bf.add("x") is True
        assert bf.add("x") is False
        assert bf.count == 1

    def test_count_increments_only_on_novel_adds(self) -> None:
        """100 distinct keys → count 100; re-adding them all moves nothing."""
        bf = BloomFilter(expected_items=10_000, fp_rate=0.01)
        for i in range(100):
            assert bf.add(f"novel-{i}") is True
        assert bf.count == 100
        for i in range(100):
            assert bf.add(f"novel-{i}") is False
        assert bf.count == 100


class TestDeterminism:
    """Same params + seed ⇒ same bits; the seed actually matters."""

    def test_same_seed_produces_identical_bit_patterns(self) -> None:
        a = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=1234)
        b = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=1234)
        for i in range(1_000):
            a.add(f"k-{i}")
            b.add(f"k-{i}")
        assert a.bits == b.bits
        assert a.count == b.count

    def test_different_seed_produces_different_bit_patterns(self) -> None:
        a = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=1234)
        c = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=4321)
        for i in range(1_000):
            a.add(f"k-{i}")
            c.add(f"k-{i}")
        assert a.bits != c.bits

    def test_indexes_stay_within_bounds(self) -> None:
        """Every derived probe position lands in [0, m), exactly k of them."""
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        for i in range(500):
            indexes = bf._indexes(f"sample-{i}")
            assert len(indexes) == bf.k
            assert all(0 <= index < bf.m for index in indexes)


class TestBitsSetCache:
    """bits_set is an O(1) cached popcount — it must never drift from truth.

    The property used to run a full ``bitarray.count()`` per read (O(m),
    ~ms-scale at the 31 Mbit access_logs size) and ``estimated_fp_rate``
    reads it on every pipeline lookup. It is now a counter that ``add``
    maintains incrementally; every test here pins the cache against the
    ground-truth popcount of the backing bitarray.
    """

    def test_cache_agrees_with_ground_truth_after_distinct_adds(self) -> None:
        """After N distinct adds, the cache equals a real bitarray.count()."""
        bf = BloomFilter(expected_items=10_000, fp_rate=0.01)
        for i in range(1_000):
            bf.add(f"cache-{i}")
        assert bf.bits_set == bf.bits.count()
        assert bf.bits_set > 0

    def test_duplicate_adds_do_not_change_bits_set(self) -> None:
        """Re-adds flip no bits, so the cache must not move (and stays true)."""
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        for i in range(100):
            bf.add(f"dup-{i}")
        before = bf.bits_set
        for i in range(100):
            bf.add(f"dup-{i}")
        assert bf.bits_set == before
        assert bf.bits_set == bf.bits.count()

    def test_same_seed_same_keys_produce_equal_bits_set(self) -> None:
        """Two filters with identical seed and inserts report the same cache."""
        a = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=1234)
        b = BloomFilter(expected_items=10_000, fp_rate=0.01, seed=1234)
        for i in range(500):
            a.add(f"k-{i}")
            b.add(f"k-{i}")
        assert a.bits_set == b.bits_set
        assert a.bits_set == a.bits.count()

    def test_empty_filter_bits_set_is_zero(self) -> None:
        bf = BloomFilter(expected_items=1_000, fp_rate=0.01)
        assert bf.bits_set == 0
        assert bf.bits_set == bf.bits.count()


class TestFPEstimates:
    """Live and theoretical estimates track the design target at capacity."""

    def test_estimates_near_target_at_design_capacity(self) -> None:
        """After n inserts into an n-capacity filter, both estimates ≈ p.

        [p/3, 3p] is a loose sanity band: at exactly design capacity both
        formulas should sit essentially on top of the 1% target.
        """
        n, p = 10_000, 0.01
        bf = BloomFilter(expected_items=n, fp_rate=p)
        for i in range(n):
            bf.add(f"cap-{i}")
        assert p / 3 < bf.estimated_fp_rate < 3 * p
        assert p / 3 < bf.theoretical_fp_rate < 3 * p
