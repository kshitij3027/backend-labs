"""Unit tests for src.persistence — the BLM1 snapshot format.

Covers the full contract: bit-exact roundtrips (bytes-level and file-level),
every rejection path returning ``None`` instead of raising (truncation,
corruption, bad magic, bad version, self-inconsistent header), and the
atomic-write guarantees (no stray ``.tmp``, overwrite-in-place, stale temp
files from a simulated crash ignored, cleanup on failure).

Tampering tests that target one specific check re-seal the blob with a
freshly computed CRC32, so the CRC check provably is NOT what fired.
"""
from __future__ import annotations

import logging
import struct
import zlib
from pathlib import Path

import pytest

from src import persistence
from src.bloom import DEFAULT_SEED, BloomFilter

# Pinned BLM1 layout offsets (little-endian). These mirror the format table
# in src/persistence.py on purpose: if the wire format drifts, these tests
# fail before any deployed snapshot stops loading.
VERSION_OFFSET = 4  # u16, right after the 4-byte magic
HEADER_OFFSET = 6  # <QHQdQQ: m, k, expected_items, fp_rate, seed, count
PAYLOAD_OFFSET = 48  # magic(4) + version(2) + header(42)
CRC_SIZE = 4  # u32 trailer
HEADER_FORMAT = "<QHQdQQ"


def make_filter(
    n: int = 200, p: float = 0.01, keys: int = 50, seed: int = DEFAULT_SEED
) -> tuple[BloomFilter, list[str]]:
    """Build a small filter with ``keys`` inserted log-entry keys."""
    bf = BloomFilter(expected_items=n, fp_rate=p, seed=seed)
    inserted = [f"log-entry-{i}" for i in range(keys)]
    for key in inserted:
        bf.add(key)
    return bf, inserted


def reseal(body: bytes) -> bytes:
    """Append a freshly computed CRC32 so only non-CRC checks can fire."""
    return body + struct.pack("<I", zlib.crc32(body))


def assert_same_filter(
    restored: BloomFilter | None,
    original: BloomFilter,
    inserted: list[str],
) -> None:
    """Restored filter must be indistinguishable from the original."""
    assert restored is not None
    # Every sizing/identity parameter restored verbatim (not recomputed).
    assert restored.m == original.m
    assert restored.k == original.k
    assert restored.count == original.count
    assert restored.seed == original.seed
    assert restored.expected_items == original.expected_items
    assert restored.fp_rate == original.fp_rate
    # The bitset itself is bit-identical.
    assert restored.bits == original.bits
    assert restored.bits.tobytes() == original.bits.tobytes()
    # The O(1) popcount cache is rebuilt at load time: it must equal the
    # ground-truth popcount AND the original's cache, so the live FP
    # estimate derived from it survives the roundtrip exactly.
    assert restored.bits_set == restored.bits.count()
    assert restored.bits_set == original.bits_set
    assert restored.estimated_fp_rate == original.estimated_fp_rate
    # Zero false negatives survive persistence: every inserted key answers True.
    for key in inserted:
        assert restored.might_contain(key) is True
    # Bit-identical behavior on keys never added: both filters give the exact
    # same answer (True only on the same false positives, if any).
    for i in range(500):
        probe = f"absent-{i}"
        assert restored.might_contain(probe) == original.might_contain(probe)


class TestByteLayout:
    """Freeze the exact BLM1 wire format: offsets, endianness, trailer."""

    def test_blob_layout_is_pinned(self) -> None:
        """dumps() emits magic@0, version@4, header@6, bits@48, CRC last."""
        bf = BloomFilter(expected_items=100, fp_rate=0.01, seed=0xABCDEF)
        bf.add("pin")
        blob = persistence.dumps(bf)

        assert blob[:4] == b"BLM1"
        assert struct.unpack_from("<H", blob, VERSION_OFFSET) == (1,)
        m, k, n, p, seed, count = struct.unpack_from(
            HEADER_FORMAT, blob, HEADER_OFFSET
        )
        assert (m, k, n, p, seed, count) == (bf.m, bf.k, 100, 0.01, 0xABCDEF, 1)
        # Payload is exactly m // 8 raw bitset bytes (m is byte-aligned).
        assert len(blob) == PAYLOAD_OFFSET + bf.m // 8 + CRC_SIZE
        assert blob[PAYLOAD_OFFSET:-CRC_SIZE] == bf.bits.tobytes()
        # Trailer is CRC32 over everything before it.
        (crc,) = struct.unpack_from("<I", blob, len(blob) - CRC_SIZE)
        assert crc == zlib.crc32(blob[:-CRC_SIZE])


class TestRoundtrip:
    """save→load and dumps→loads reproduce the filter bit-for-bit."""

    def test_save_load_roundtrip_preserves_membership(self, tmp_path: Path) -> None:
        """n=5000 @ p=0.01 with 2000 keys survives a disk roundtrip exactly."""
        bf, inserted = make_filter(n=5_000, p=0.01, keys=2_000)
        target = tmp_path / "error_logs.bloom"

        persistence.save(bf, target)
        restored = persistence.load(target)

        assert_same_filter(restored, bf, inserted)

    def test_dumps_loads_roundtrip_without_files(self) -> None:
        """The byte primitives roundtrip on their own (custom seed travels)."""
        bf, inserted = make_filter(n=1_000, p=0.01, keys=300, seed=1234)

        blob = persistence.dumps(bf)
        restored = persistence.loads(blob)

        assert_same_filter(restored, bf, inserted)
        assert restored is not None
        assert restored.seed == 1234
        # Re-serializing the restored filter is byte-identical: the format
        # has a single canonical encoding per filter state.
        assert persistence.dumps(restored) == blob


class TestRejection:
    """Every invalid input path returns None — corrupt input never raises."""

    def test_load_missing_file_returns_none(self, tmp_path: Path) -> None:
        assert persistence.load(tmp_path / "never-written.bloom") is None

    def test_truncated_snapshot_returns_none(self, tmp_path: Path) -> None:
        """A half-written file (no atomic rename would produce one) is rejected."""
        bf, _ = make_filter()
        target = tmp_path / "snap.bloom"
        persistence.save(bf, target)

        data = target.read_bytes()
        target.write_bytes(data[: len(data) // 2])

        assert persistence.load(target) is None

    def test_below_structural_minimum_returns_none(self) -> None:
        """Empty/tiny inputs fail the length check before any parsing."""
        bf, _ = make_filter()
        blob = persistence.dumps(bf)
        assert persistence.loads(b"") is None
        assert persistence.loads(blob[:51]) is None  # one short of the 52-byte min

    def test_corrupt_payload_byte_returns_none_and_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """One flipped byte mid-payload is caught by the CRC and warned about."""
        bf, _ = make_filter(n=1_000, p=0.01, keys=300)
        target = tmp_path / "snap.bloom"
        persistence.save(bf, target)

        data = bytearray(target.read_bytes())
        flip_at = PAYLOAD_OFFSET + (len(data) - PAYLOAD_OFFSET - CRC_SIZE) // 2
        data[flip_at] ^= 0xFF
        target.write_bytes(bytes(data))

        with caplog.at_level(logging.WARNING, logger=persistence.__name__):
            assert persistence.load(target) is None
        assert any(
            "CRC mismatch" in record.getMessage() for record in caplog.records
        )

    def test_bad_magic_returns_none(self) -> None:
        bf, _ = make_filter()
        blob = persistence.dumps(bf)

        # As the prompt for a torn header: raw magic swap (CRC also stale).
        assert persistence.loads(b"XXXX" + blob[4:]) is None
        # Re-sealed variant: CRC is valid, so the *magic* check is what fires.
        assert persistence.loads(reseal(b"XXXX" + blob[4:-CRC_SIZE])) is None

    def test_bad_version_returns_none(self) -> None:
        """Version 99 with a recomputed CRC: only the version check can fire."""
        bf, _ = make_filter()
        body = persistence.dumps(bf)[:-CRC_SIZE]

        tampered = (
            body[:VERSION_OFFSET]
            + struct.pack("<H", 99)
            + body[VERSION_OFFSET + 2 :]
        )
        assert persistence.loads(reseal(tampered)) is None

    def test_header_payload_length_mismatch_returns_none(self) -> None:
        """CRC-valid blob whose header m disagrees with the payload length."""
        bf, _ = make_filter()
        body = bytearray(persistence.dumps(bf)[:-CRC_SIZE])

        # Splice m to a still-byte-aligned but wrong value: m//8 no longer
        # matches the payload length, and only that check can fire.
        struct.pack_into("<Q", body, HEADER_OFFSET, bf.m + 8)
        assert persistence.loads(reseal(bytes(body))) is None

    def test_header_m_not_byte_aligned_returns_none(self) -> None:
        """CRC-valid blob with m % 8 != 0 is structurally impossible — rejected."""
        bf, _ = make_filter()
        body = bytearray(persistence.dumps(bf)[:-CRC_SIZE])

        struct.pack_into("<Q", body, HEADER_OFFSET, bf.m + 1)
        assert persistence.loads(reseal(bytes(body))) is None


class TestAtomicity:
    """tmp+fsync+rename: the live path always holds one complete snapshot."""

    def test_save_leaves_no_tmp_file(self, tmp_path: Path) -> None:
        bf, _ = make_filter()
        target = tmp_path / "snap.bloom"

        persistence.save(bf, target)

        assert target.exists()
        assert list(tmp_path.glob("*.tmp")) == []

    def test_save_over_existing_snapshot_loads_new_content(
        self, tmp_path: Path
    ) -> None:
        """A second save atomically replaces the first; load sees the new bits."""
        target = tmp_path / "snap.bloom"
        old, _ = make_filter(keys=10)
        persistence.save(old, target)

        new = BloomFilter(expected_items=200, fp_rate=0.01)
        new_keys = [f"new-key-{i}" for i in range(40)]
        for key in new_keys:
            new.add(key)
        persistence.save(new, target)

        restored = persistence.load(target)
        assert_same_filter(restored, new, new_keys)
        assert restored is not None
        assert restored.bits != old.bits

    def test_stale_tmp_from_simulated_crash_is_ignored(
        self, tmp_path: Path
    ) -> None:
        """Garbage at <path>.tmp (crash before rename) never shadows the live file."""
        bf, inserted = make_filter()
        target = tmp_path / "snap.bloom"
        persistence.save(bf, target)

        # Crash simulation: a writer died after writing the temp file but
        # before the atomic rename. The live snapshot must stay readable.
        (tmp_path / "snap.bloom.tmp").write_bytes(b"\x00garbage half-write\xff" * 7)

        assert_same_filter(persistence.load(target), bf, inserted)

    def test_write_atomic_creates_parent_dirs(self, tmp_path: Path) -> None:
        bf, inserted = make_filter()
        target = tmp_path / "data" / "snapshots" / "snap.bloom"

        persistence.save(bf, target)

        assert_same_filter(persistence.load(target), bf, inserted)

    def test_failed_rename_cleans_up_tmp_and_reraises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the final rename fails, no .tmp litter survives and the error surfaces."""
        target = tmp_path / "snap.bloom"

        def explode(src: object, dst: object) -> None:
            raise OSError("simulated crash at rename")

        monkeypatch.setattr(persistence.os, "replace", explode)
        with pytest.raises(OSError, match="simulated crash at rename"):
            persistence.write_atomic(b"some snapshot bytes", target)

        assert not target.exists()
        assert list(tmp_path.glob("*.tmp")) == []
