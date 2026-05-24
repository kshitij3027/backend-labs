"""Unit tests for :mod:`src.storage.compressor`.

These tests exercise the streaming zstd helpers in isolation: no
catalog, no mover, no scheduler. They use ``tmp_path`` (pytest's
per-test temp dir) so each case gets a fresh filesystem slice and
nothing leaks between runs.

The round-trip assertions are the contract: any future change to the
chunk size, compressor settings, or library version must still produce
byte-identical decompressed output. The "level 19 <= level 3" check
guards the storage-savings promise the archive tier is built on.
"""

from __future__ import annotations

import os

import pytest

from src.storage.compressor import (
    DEFAULT_ARCHIVE_LEVEL,
    DEFAULT_COLD_LEVEL,
    compress_file,
    decompress_file,
)


def _write_bytes(path, data: bytes) -> None:
    """Helper: write ``data`` to ``path`` and ensure it hits the disk."""
    path.write_bytes(data)


def test_round_trip_level_3(tmp_path):
    """Cold-tier level (3) must round-trip identically."""
    # ``os.urandom`` produces incompressible bytes, which is the
    # adversarial case for any compressor — if round-trip works here
    # it works for structured log data too.
    payload = os.urandom(50 * 1024)  # 50 KiB
    src = tmp_path / "src.bin"
    compressed = tmp_path / "compressed.zst"
    roundtrip = tmp_path / "roundtrip.bin"
    _write_bytes(src, payload)

    compress_file(src, compressed, level=3)
    decompress_file(compressed, roundtrip)

    assert roundtrip.read_bytes() == payload


def test_round_trip_level_19(tmp_path):
    """Archive-tier level (19) must round-trip identically."""
    payload = os.urandom(50 * 1024)  # 50 KiB
    src = tmp_path / "src.bin"
    compressed = tmp_path / "compressed.zst"
    roundtrip = tmp_path / "roundtrip.bin"
    _write_bytes(src, payload)

    compress_file(src, compressed, level=19)
    decompress_file(compressed, roundtrip)

    assert roundtrip.read_bytes() == payload


def test_compress_returns_byte_count(tmp_path):
    """The return value of compress_file must equal dst.stat().st_size."""
    src = tmp_path / "src.bin"
    dst = tmp_path / "out.zst"
    _write_bytes(src, b"hello world " * 100)

    written = compress_file(src, dst, level=DEFAULT_COLD_LEVEL)

    assert isinstance(written, int)
    assert written == dst.stat().st_size


def test_higher_level_smaller_or_equal_output(tmp_path):
    """Level 19 must not produce a larger artefact than level 3 on
    real-world-shaped input.

    On tiny or already-random payloads zstd levels can tie or even
    invert (overhead of the bigger window can hurt), so we feed enough
    compressible, structured bytes to make the slower preset clearly
    pay off.
    """
    # ~25 KiB of repeating JSON-ish lines — extremely compressible,
    # exactly the shape of real log segments.
    payload = (b'{"level":"INFO","msg":"user logged in","uid":42}\n' * 500)
    assert len(payload) >= 4096

    src = tmp_path / "src.bin"
    out_low = tmp_path / "low.zst"
    out_high = tmp_path / "high.zst"
    _write_bytes(src, payload)

    size_low = compress_file(src, out_low, level=3)
    size_high = compress_file(src, out_high, level=19)

    assert size_high <= size_low


def test_decompress_returns_byte_count(tmp_path):
    """decompress_file must return an int equal to the output file size
    and equal to the original source size."""
    payload = b"some structured logs " * 200
    src = tmp_path / "src.bin"
    compressed = tmp_path / "c.zst"
    roundtrip = tmp_path / "rt.bin"
    _write_bytes(src, payload)
    compress_file(src, compressed, level=DEFAULT_COLD_LEVEL)

    written = decompress_file(compressed, roundtrip)

    assert isinstance(written, int)
    assert written == roundtrip.stat().st_size
    assert written == len(payload)


def test_compress_file_handles_empty_input(tmp_path):
    """A zero-byte source must compress and decompress cleanly back to
    zero bytes. Empty segments can show up at scheduler boundaries."""
    src = tmp_path / "empty.bin"
    compressed = tmp_path / "empty.zst"
    roundtrip = tmp_path / "empty_rt.bin"
    _write_bytes(src, b"")

    compressed_size = compress_file(src, compressed, level=DEFAULT_COLD_LEVEL)
    decompressed_size = decompress_file(compressed, roundtrip)

    # The compressed file is non-empty (zstd frame headers) but the
    # round-tripped output must match the empty source exactly.
    assert compressed_size >= 0
    assert decompressed_size == 0
    assert roundtrip.read_bytes() == b""


def test_constants_are_correct():
    """The two preset constants the applier reads must hold their
    documented values."""
    assert DEFAULT_COLD_LEVEL == 3
    assert DEFAULT_ARCHIVE_LEVEL == 19
