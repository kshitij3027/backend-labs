"""Unit tests for src.scalable + the ``SBF1`` container in src.persistence.

Covers the Almeida et al. design end to end: constructor validation, the
paper-faithful geometric error budget (``fp_i = target * (1-r) * r**i`` —
the series that sums to *exactly* the target), geometric capacity growth,
zero false negatives across growth, dedup-on-add semantics, the compound FP
estimate staying under target after heavy overfill, and the SBF1 container
(pinned byte layout, roundtrips, every rejection path returning ``None``).

Determinism note: murmur3 hashing is fully deterministic for a fixed seed,
so membership and count outcomes here are exact, not statistical — each
key-namespace's behavior (including which namespaces produce zero
compound-false-positive dedup skips) was verified once against the pinned
mmh3/bitarray builds. Namespaces where a handful of the inserted keys *are*
compound false positives (and therefore get dedup-skipped, by design) avoid
exact-count literals and capture-and-compare instead.

Container tampering tests that target one specific check re-seal the blob
with a freshly computed outer CRC32, so the CRC check provably is NOT what
fired (same convention as test_persistence.py).
"""
from __future__ import annotations

import logging
import struct
import zlib
from pathlib import Path

import pytest

from src import persistence
from src.bloom import DEFAULT_SEED, BloomFilter
from src.scalable import ScalableBloomFilter

# Pinned SBF1 layout offsets (little-endian). These mirror the format table
# in src/persistence.py on purpose: if the wire format drifts, these tests
# fail before any deployed snapshot stops loading.
SBF_VERSION_OFFSET = 4  # u16, right after the 4-byte magic
SBF_PARAMS_OFFSET = 6  # <QdHdQH: n0, target, growth, tightening, seed, slices
SBF_PARAMS_FORMAT = "<QdHdQH"
SBF_TIGHTENING_OFFSET = 24  # f64, third params field
SBF_SLICE_COUNT_OFFSET = 40  # u16, last params field
SBF_TABLE_OFFSET = 42  # magic(4) + version(2) + params(36)
CRC_SIZE = 4  # u32 trailer

#: Safety valve for grow-until loops — a broken append path must fail the
#: assertion inside the loop, never hang the suite.
GROWTH_CAP = 100_000


def expected_fp(target: float, tightening: float, index: int) -> float:
    """Slice ``index``'s budget, written exactly like the implementation.

    Same operations in the same order as ``ScalableBloomFilter._new_slice``
    so equality checks are bit-exact, not approximate.
    """
    return target * (1.0 - tightening) * tightening**index


def grow_to(sbf: ScalableBloomFilter, slices: int, prefix: str) -> int:
    """Insert distinct ``prefix``-namespaced keys until ``slices`` exist."""
    inserted = 0
    while sbf.slice_count < slices:
        sbf.add(f"{prefix}-{inserted}")
        inserted += 1
        assert inserted <= GROWTH_CAP, "filter failed to grow — append broken"
    return inserted


def make_grown_sbf() -> tuple[ScalableBloomFilter, list[str]]:
    """A 3-slice filter (100/0.01 overfilled with 400 'round-' keys)."""
    sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
    keys = [f"round-{i}" for i in range(400)]
    for key in keys:
        sbf.add(key)
    assert sbf.slice_count == 3
    return sbf, keys


def reseal(body: bytes) -> bytes:
    """Append a freshly computed CRC32 so only non-CRC checks can fire."""
    return body + struct.pack("<I", zlib.crc32(body))


class TestValidation:
    """Constructor rejects every out-of-domain parameter."""

    @pytest.mark.parametrize("initial", [0, -1, -100])
    def test_bad_initial_capacity(self, initial: int) -> None:
        with pytest.raises(ValueError, match="initial_capacity"):
            ScalableBloomFilter(initial, 0.01)

    @pytest.mark.parametrize("target", [0.0, 1.0, -0.5, 2.0])
    def test_bad_target_fp_rate(self, target: float) -> None:
        with pytest.raises(ValueError, match="target_fp_rate"):
            ScalableBloomFilter(100, target)

    @pytest.mark.parametrize("growth", [1, 0, -2])
    def test_bad_growth(self, growth: int) -> None:
        with pytest.raises(ValueError, match="growth"):
            ScalableBloomFilter(100, 0.01, growth=growth)

    @pytest.mark.parametrize("tightening", [0.0, 1.0, -0.1, 1.5])
    def test_bad_tightening(self, tightening: float) -> None:
        with pytest.raises(ValueError, match="tightening"):
            ScalableBloomFilter(100, 0.01, tightening=tightening)

    def test_slice_zero_exists_eagerly(self) -> None:
        """A fresh filter is immediately usable: one empty slice, no growth."""
        sbf = ScalableBloomFilter(100, 0.01)
        assert sbf.slice_count == 1
        assert sbf.count == 0
        assert sbf.might_contain("anything") is False


class TestBudgetMath:
    """The geometric series fp_i = target·(1−r)·r^i, capacities n0·s^i."""

    def test_slice_budgets_follow_the_paper_series(self) -> None:
        """(initial=100, target=0.01): fp 0.0015, 0.0015·0.85, 0.0015·0.85²."""
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        grow_to(sbf, 3, "budget")

        assert [s.expected_items for s in sbf.slices] == [100, 200, 400]
        # Slice 0 pays the (1 - r) down-payment: 0.01 * 0.15 = 0.0015, NOT
        # the full 0.01 a naive "start at target" variant would grant.
        assert sbf.slices[0].fp_rate == pytest.approx(0.0015)
        assert sbf.slices[1].fp_rate == pytest.approx(0.0015 * 0.85)
        assert sbf.slices[2].fp_rate == pytest.approx(0.0015 * 0.85**2)
        # Bit-exact against the implementation's own expression.
        for i, s in enumerate(sbf.slices):
            assert s.fp_rate == expected_fp(0.01, 0.85, i)

    def test_total_granted_budget_never_reaches_target(self) -> None:
        """Any finite prefix of the geometric series stays strictly below
        the target — the compound guarantee holds at every growth stage."""
        sbf = ScalableBloomFilter(initial_capacity=10, target_fp_rate=0.01)
        grow_to(sbf, 5, "series")
        granted = sum(s.fp_rate for s in sbf.slices)
        assert granted < 0.01
        # And the tail it converges to is exactly the target: the partial
        # sum of a geometric series with ratio r is target * (1 - r**n).
        assert granted == pytest.approx(0.01 * (1 - 0.85**5))

    def test_non_default_growth_and_tightening_respected(self) -> None:
        sbf = ScalableBloomFilter(
            50, 0.02, growth=3, tightening=0.7, seed=0xABCDEF
        )
        grow_to(sbf, 2, "layout")
        assert [s.expected_items for s in sbf.slices] == [50, 150]
        for i, s in enumerate(sbf.slices):
            assert s.fp_rate == expected_fp(0.02, 0.7, i)

    def test_slices_hash_with_independent_seeds(self) -> None:
        """Slice i seeds murmur with seed+i — identical seeds would correlate
        false positives across slices and void the compound product bound."""
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        grow_to(sbf, 3, "seeds")
        assert sbf.slices[0].seed != sbf.slices[1].seed
        assert [s.seed for s in sbf.slices] == [
            DEFAULT_SEED + i for i in range(3)
        ]


class TestGrowth:
    """Overfilling appends geometrically larger slices."""

    def test_overfill_4x_grows_slices_geometrically(self) -> None:
        """400 distinct keys into n0=100: crossings at 100 and 300 admits.

        The 'growth-' namespace is verified to produce zero compound-FP
        dedup skips with the default seed, so every add returns True and
        the count matches the inserted count exactly.
        """
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        results = [sbf.add(f"growth-{i}") for i in range(400)]

        assert all(results)
        assert sbf.count == 400  # distinct keys all counted exactly once
        assert sbf.slice_count == 3
        assert [s.expected_items for s in sbf.slices] == [
            100 * 2**i for i in range(sbf.slice_count)
        ]
        # Landing pattern: slice fills to capacity, then the series moves on.
        assert [s.count for s in sbf.slices] == [100, 200, 100]

    def test_aggregate_properties_sum_over_slices(self) -> None:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        for i in range(400):
            sbf.add(f"growth-{i}")

        assert sbf.capacity == sum(s.expected_items for s in sbf.slices) == 700
        assert sbf.count == sum(s.count for s in sbf.slices)
        assert sbf.memory_bytes == sum(s.memory_bytes for s in sbf.slices)
        assert sbf.initial_capacity == 100
        assert sbf.target_fp_rate == 0.01
        assert sbf.growth == 2
        assert sbf.tightening == 0.85
        assert sbf.seed == DEFAULT_SEED
        # The slices view is a read-only tuple snapshot, oldest first.
        assert isinstance(sbf.slices, tuple)
        assert all(isinstance(s, BloomFilter) for s in sbf.slices)


class TestZeroFalseNegatives:
    """The headline guarantee survives growth across many slices."""

    def test_every_inserted_key_answers_true_after_heavy_growth(self) -> None:
        """1000 keys into n0=100 spread over 4 slices — none ever lost.

        Holds for *every* key passed to add, including the few that were
        compound false positives at insert time and got dedup-skipped: those
        already answered True everywhere, which is why they were skipped.
        """
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        keys = [f"zero-fn-{i}" for i in range(1000)]
        for key in keys:
            sbf.add(key)

        assert sbf.slice_count >= 3  # growth definitely happened
        assert all(sbf.might_contain(key) for key in keys)


class TestDedup:
    """add() checks all slices first — duplicates can never drive growth."""

    def test_readding_a_key_is_a_noop(self) -> None:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        assert sbf.add("x") is True
        assert sbf.add("x") is False
        assert sbf.count == 1
        assert sbf.slice_count == 1

    def test_readding_existing_keys_never_grows_slices(self) -> None:
        """Replaying 1000 already-seen keys changes nothing anywhere.

        This is the property the dedup pre-check exists for: without it,
        every replay would re-insert into the newest slice and a
        duplicate-heavy log stream would grow memory without bound.
        """
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        keys = [f"dedup-{i}" for i in range(1000)]
        for key in keys:
            sbf.add(key)

        count = sbf.count
        slice_count = sbf.slice_count
        per_slice_counts = [s.count for s in sbf.slices]
        per_slice_bits = [s.bits_set for s in sbf.slices]

        results = [sbf.add(key) for key in keys]

        assert not any(results)  # every re-add reports "already present"
        assert sbf.count == count
        assert sbf.slice_count == slice_count
        assert [s.count for s in sbf.slices] == per_slice_counts
        assert [s.bits_set for s in sbf.slices] == per_slice_bits  # no bit moved


class TestCompoundFalsePositives:
    """The whole point of the budget series: compound FP ≤ target, always."""

    def test_compound_estimate_bounded_by_target_after_overfill(self) -> None:
        """4× overfill (2000 into n0=500): estimate stays ≤ 0.01.

        Each filled slice contributes ≈ its budget fp_i, and the budgets sum
        to ≤ target by construction (deterministic value here ≈ 0.0029).
        """
        sbf = ScalableBloomFilter(initial_capacity=500, target_fp_rate=0.01)
        for i in range(2000):
            sbf.add(f"compound-{i}")

        assert sbf.slice_count >= 3
        assert 0.0 < sbf.compound_estimated_fp <= 0.01

    def test_observed_fp_rate_after_overfill_stays_near_target(self) -> None:
        """20k disjoint probes against a 4×-overfilled filter: < 1.5× target.

        Deterministic with the default seed: 72 of 20000 probes false-
        positive (0.0036) — comfortably under the 0.015 ceiling and in line
        with the ≈0.0029 compound estimate.
        """
        sbf = ScalableBloomFilter(initial_capacity=500, target_fp_rate=0.01)
        for i in range(2000):
            sbf.add(f"observed-{i}")

        false_positives = sum(
            1 for i in range(20_000) if sbf.might_contain(f"disjoint-{i}")
        )
        assert false_positives / 20_000 < 0.015

    def test_compound_estimate_is_the_union_bound_of_slice_estimates(self) -> None:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        for i in range(250):
            sbf.add(f"union-{i}")

        product = 1.0
        for s in sbf.slices:
            product *= 1.0 - s.estimated_fp_rate
        assert sbf.compound_estimated_fp == 1.0 - product
        # Strictly above any single slice's estimate (union over slices)...
        assert sbf.compound_estimated_fp >= max(
            s.estimated_fp_rate for s in sbf.slices
        )
        # ...and at most their sum (sub-additivity).
        assert sbf.compound_estimated_fp <= sum(
            s.estimated_fp_rate for s in sbf.slices
        )


class TestBitsSetCacheAcrossGrowth:
    """The O(1) bits_set cache must hold for every slice the series appends.

    compound_estimated_fp reads each slice's cached popcount per call, so a
    slice whose cache drifted from the real bitarray.count() would skew the
    pipeline's fallback gauge silently.
    """

    def test_every_slice_cache_matches_real_popcount_after_overfill(self) -> None:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        for i in range(400):
            sbf.add(f"cache-{i}")
        assert sbf.slice_count >= 3  # growth definitely happened

        for s in sbf.slices:
            assert s.bits_set == s.bits.count()
        # Reads are pure: repeated compound estimates return the identical
        # value (no hidden state moves on the query path).
        first = sbf.compound_estimated_fp
        assert sbf.compound_estimated_fp == first
        assert sbf.compound_estimated_fp == first


class TestStats:
    """stats() is the JSON-friendly contract /stats (C8) builds on."""

    def test_stats_shape_and_consistency(self) -> None:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        for i in range(150):
            sbf.add(f"stats-{i}")
        stats = sbf.stats()

        assert set(stats) == {
            "target_fp_rate",
            "compound_estimated_fp",
            "count",
            "capacity",
            "slice_count",
            "memory_bytes",
            "growth",
            "tightening",
            "slices",
        }
        assert stats["target_fp_rate"] == 0.01
        assert stats["growth"] == 2
        assert stats["tightening"] == 0.85
        assert stats["count"] == sbf.count
        assert stats["capacity"] == sbf.capacity
        assert stats["memory_bytes"] == sbf.memory_bytes
        assert stats["compound_estimated_fp"] == sbf.compound_estimated_fp

        # The nested slices list reuses BloomFilter.stats() verbatim.
        assert isinstance(stats["slices"], list)
        assert stats["slice_count"] == sbf.slice_count == len(stats["slices"])
        for nested, s in zip(stats["slices"], sbf.slices):
            assert nested == s.stats()
        # Aggregates really are the sums of the per-slice numbers.
        assert stats["count"] == sum(n["count"] for n in stats["slices"])
        assert stats["memory_bytes"] == sum(
            n["memory_bytes"] for n in stats["slices"]
        )


class TestSBF1ByteLayout:
    """Freeze the exact SBF1 wire format: offsets, endianness, trailer."""

    def test_container_layout_is_pinned(self) -> None:
        """magic@0, version@4, params@6, length-prefixed BLM1 table@42, CRC."""
        sbf = ScalableBloomFilter(
            50, 0.02, growth=3, tightening=0.7, seed=0xABCDEF
        )
        grow_to(sbf, 2, "layout")
        blob = persistence.dumps_scalable(sbf)

        assert blob[:4] == b"SBF1"
        assert struct.unpack_from("<H", blob, SBF_VERSION_OFFSET) == (1,)
        assert struct.unpack_from(SBF_PARAMS_FORMAT, blob, SBF_PARAMS_OFFSET) == (
            50,
            0.02,
            3,
            0.7,
            0xABCDEF,
            sbf.slice_count,
        )
        # Slice table: each entry is a u32 length + that slice's exact BLM1
        # encoding (the container invents no second bitset format).
        offset = SBF_TABLE_OFFSET
        for s in sbf.slices:
            (length,) = struct.unpack_from("<I", blob, offset)
            offset += 4
            assert blob[offset : offset + length] == persistence.dumps(s)
            offset += length
        # The table consumes everything up to the CRC trailer, which covers
        # every byte before it.
        assert offset == len(blob) - CRC_SIZE
        (crc,) = struct.unpack_from("<I", blob, len(blob) - CRC_SIZE)
        assert crc == zlib.crc32(blob[:-CRC_SIZE])


class TestSBF1Roundtrip:
    """save_scalable→load_scalable reproduces the whole series exactly."""

    def test_save_load_roundtrip_preserves_everything(
        self, tmp_path: Path
    ) -> None:
        sbf, keys = make_grown_sbf()
        target = tmp_path / "error_logs.sbf"

        persistence.save_scalable(sbf, target)
        restored = persistence.load_scalable(target)

        assert restored is not None
        # Series parameters travel verbatim (they drive future growth).
        assert restored.initial_capacity == sbf.initial_capacity
        assert restored.target_fp_rate == sbf.target_fp_rate
        assert restored.growth == sbf.growth
        assert restored.tightening == sbf.tightening
        assert restored.seed == sbf.seed
        # The slice series is reproduced slice-for-slice, bit-for-bit.
        assert restored.slice_count == sbf.slice_count
        for r, o in zip(restored.slices, sbf.slices):
            assert (r.m, r.k, r.count, r.seed) == (o.m, o.k, o.count, o.seed)
            assert (r.expected_items, r.fp_rate) == (o.expected_items, o.fp_rate)
            assert r.bits == o.bits
        # Membership and the live estimate are indistinguishable.
        assert restored.count == sbf.count
        assert all(restored.might_contain(key) for key in keys)
        assert restored.compound_estimated_fp == sbf.compound_estimated_fp
        # No stray temp files from the atomic write.
        assert list(tmp_path.glob("*.tmp")) == []

    def test_dumps_loads_roundtrip_without_files(self) -> None:
        """Byte primitives roundtrip on their own; non-default params travel."""
        sbf = ScalableBloomFilter(
            50, 0.02, growth=3, tightening=0.7, seed=0xABCDEF
        )
        keys = [f"layout-{i}" for i in range(120)]
        for key in keys:
            sbf.add(key)

        blob = persistence.dumps_scalable(sbf)
        restored = persistence.loads_scalable(blob)

        assert restored is not None
        assert (restored.growth, restored.tightening) == (3, 0.7)
        assert restored.seed == 0xABCDEF
        assert restored.slice_count == sbf.slice_count
        assert all(restored.might_contain(key) for key in keys)
        # Re-serializing the restored filter is byte-identical: the format
        # has a single canonical encoding per filter state.
        assert persistence.dumps_scalable(restored) == blob

    def test_restored_filter_keeps_growing_with_the_stored_params(self) -> None:
        """The restored series must grow exactly like the original would.

        This is what restoring the *parameters* (not just the slices) buys:
        the next slice appended after a reload gets the correct capacity,
        budget, and seed for its index in the series.
        """
        sbf, _ = make_grown_sbf()  # 3 slices, slice 2 partially filled
        restored = persistence.loads_scalable(persistence.dumps_scalable(sbf))
        assert restored is not None

        grow_to(restored, 4, "post")
        appended = restored.slices[3]
        assert appended.expected_items == 100 * 2**3
        assert appended.fp_rate == expected_fp(0.01, 0.85, 3)
        assert appended.seed == DEFAULT_SEED + 3


class TestSBF1Rejection:
    """Every invalid container path returns None — never raises."""

    def make_blob(self) -> bytes:
        sbf = ScalableBloomFilter(initial_capacity=100, target_fp_rate=0.01)
        for i in range(150):
            sbf.add(f"reject-{i}")
        assert sbf.slice_count >= 2
        return persistence.dumps_scalable(sbf)

    def test_load_missing_file_returns_none(self, tmp_path: Path) -> None:
        assert persistence.load_scalable(tmp_path / "never-written.sbf") is None

    def test_corrupt_byte_returns_none_and_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """One flipped byte mid-container is caught by the outer CRC."""
        blob = bytearray(self.make_blob())
        blob[len(blob) // 2] ^= 0xFF
        target = tmp_path / "snap.sbf"
        target.write_bytes(bytes(blob))

        with caplog.at_level(logging.WARNING, logger=persistence.__name__):
            assert persistence.load_scalable(target) is None
        assert any(
            "CRC mismatch" in record.getMessage() for record in caplog.records
        )

    def test_truncated_container_returns_none(self, tmp_path: Path) -> None:
        blob = self.make_blob()
        target = tmp_path / "snap.sbf"
        target.write_bytes(blob[: len(blob) // 2])
        assert persistence.load_scalable(target) is None

    def test_below_structural_minimum_returns_none(self) -> None:
        blob = self.make_blob()
        assert persistence.loads_scalable(b"") is None
        assert persistence.loads_scalable(blob[:45]) is None  # min is 46

    def test_bad_magic_returns_none(self) -> None:
        blob = self.make_blob()
        # Raw magic swap (CRC also stale) and a re-sealed variant where the
        # CRC is valid so the *magic* check provably is what fires.
        assert persistence.loads_scalable(b"XXXX" + blob[4:]) is None
        assert (
            persistence.loads_scalable(reseal(b"XXXX" + blob[4:-CRC_SIZE]))
            is None
        )
        # A plain BLM1 blob is not an SBF1 container (and vice versa).
        bf = BloomFilter(expected_items=100, fp_rate=0.01)
        assert persistence.loads_scalable(persistence.dumps(bf)) is None

    def test_bad_version_returns_none(self) -> None:
        body = bytearray(self.make_blob()[:-CRC_SIZE])
        struct.pack_into("<H", body, SBF_VERSION_OFFSET, 99)
        assert persistence.loads_scalable(reseal(bytes(body))) is None

    def test_implausible_params_returns_none(self) -> None:
        """CRC-valid container whose tightening is out of (0, 1)."""
        body = bytearray(self.make_blob()[:-CRC_SIZE])
        struct.pack_into("<d", body, SBF_TIGHTENING_OFFSET, 1.5)
        assert persistence.loads_scalable(reseal(bytes(body))) is None

    def test_slice_count_mismatch_returns_none(self) -> None:
        """CRC-valid container whose header disagrees with the slice table."""
        blob = self.make_blob()
        (declared,) = struct.unpack_from("<H", blob, SBF_SLICE_COUNT_OFFSET)

        # One slice too many: the table walk runs out of bytes.
        body = bytearray(blob[:-CRC_SIZE])
        struct.pack_into("<H", body, SBF_SLICE_COUNT_OFFSET, declared + 1)
        assert persistence.loads_scalable(reseal(bytes(body))) is None

        # One slice too few: trailing unconsumed bytes before the CRC.
        body = bytearray(blob[:-CRC_SIZE])
        struct.pack_into("<H", body, SBF_SLICE_COUNT_OFFSET, declared - 1)
        assert persistence.loads_scalable(reseal(bytes(body))) is None

    def test_corrupt_inner_blob_rejected_despite_valid_outer_crc(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A bad slice rejects the WHOLE container (no partial restores —
        a missing slice would mean false negatives for every key in it)."""
        body = bytearray(self.make_blob()[:-CRC_SIZE])
        # Flip a byte inside slice 0's embedded BLM1 (just past its u32
        # length prefix); the outer CRC is then re-sealed, so only the inner
        # BLM1 validation can be what fires.
        body[SBF_TABLE_OFFSET + 4 + 10] ^= 0xFF

        with caplog.at_level(logging.WARNING, logger=persistence.__name__):
            assert persistence.loads_scalable(reseal(bytes(body))) is None
        assert any(
            "slice 0" in record.getMessage() for record in caplog.records
        )


class TestBLM1Regression:
    """The SBF1 extension must not disturb the existing single-filter format."""

    def test_single_filter_save_load_still_works(self, tmp_path: Path) -> None:
        bf = BloomFilter(expected_items=200, fp_rate=0.01)
        keys = [f"regression-{i}" for i in range(50)]
        for key in keys:
            bf.add(key)
        target = tmp_path / "single.bloom"

        persistence.save(bf, target)
        restored = persistence.load(target)

        assert restored is not None
        assert (restored.m, restored.k, restored.count) == (bf.m, bf.k, bf.count)
        assert restored.bits == bf.bits
        assert all(restored.might_contain(key) for key in keys)
        assert persistence.dumps(restored) == persistence.dumps(bf)
