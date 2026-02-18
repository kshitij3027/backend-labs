"""Tests for src/compression.py — CompressionHandler and CompressionResult."""

import hashlib
import json
import threading
import pytest

from src.compression import CompressionHandler, CompressionResult


# ── Helper: repetitive log-like data ────────────────────────────────

def _make_log_data(n_entries: int = 200) -> bytes:
    """Create a large JSON string of repeated log entries (highly compressible)."""
    entries = [
        {
            "timestamp": "2026-02-17T12:00:00Z",
            "level": "INFO",
            "service": "auth-service",
            "message": f"User logged in successfully (request #{i})",
            "request_id": f"req-{i:06d}",
            "metadata": {"ip": "192.168.1.1", "user_agent": "Mozilla/5.0"},
        }
        for i in range(n_entries)
    ]
    return json.dumps(entries).encode("utf-8")


def _make_moderate_entropy_data(size: int = 100_000) -> bytes:
    """Create data with moderate entropy where compression levels make a difference.

    Uses pseudo-random hashes mixed with structured text so that different
    compression levels produce noticeably different output sizes.
    """
    chunks = []
    i = 0
    while len(b"".join(chunks)) < size:
        h = hashlib.md5(str(i).encode()).hexdigest()
        chunks.append(f"line {i}: hash={h} status=OK ts=2026-02-17T12:{i % 60:02d}:00Z\n".encode())
        i += 1
    return b"".join(chunks)[:size]


# ── Round-trip tests ────────────────────────────────────────────────

class TestRoundTripGzip:
    def test_compress_then_decompress_returns_original(self):
        handler = CompressionHandler(algorithm="gzip", level=6)
        original = _make_log_data(100)
        result = handler.compress(original)
        restored = handler.decompress(result.data, result.algorithm)
        assert restored == original

    def test_small_and_large_payloads(self):
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=0)
        for size in [300, 1_000, 50_000]:
            original = b"A" * size
            result = handler.compress(original)
            restored = handler.decompress(result.data, result.algorithm)
            assert restored == original


class TestRoundTripZlib:
    def test_compress_then_decompress_returns_original(self):
        handler = CompressionHandler(algorithm="zlib", level=6)
        original = _make_log_data(100)
        result = handler.compress(original)
        restored = handler.decompress(result.data, result.algorithm)
        assert restored == original

    def test_small_and_large_payloads(self):
        handler = CompressionHandler(algorithm="zlib", level=6, bypass_threshold=0)
        for size in [300, 1_000, 50_000]:
            original = b"B" * size
            result = handler.compress(original)
            restored = handler.decompress(result.data, result.algorithm)
            assert restored == original


# ── Compression ratio ───────────────────────────────────────────────

class TestCompressionRatio:
    def test_gzip_ratio_greater_than_one_for_repetitive_data(self):
        handler = CompressionHandler(algorithm="gzip", level=6)
        data = _make_log_data(200)
        result = handler.compress(data)
        assert result.ratio > 1.0
        assert result.compressed_size < result.original_size

    def test_zlib_ratio_greater_than_one_for_repetitive_data(self):
        handler = CompressionHandler(algorithm="zlib", level=6)
        data = _make_log_data(200)
        result = handler.compress(data)
        assert result.ratio > 1.0
        assert result.compressed_size < result.original_size


# ── Level 1 vs level 9 produce different sizes ─────────────────────

class TestLevelDifference:
    def test_gzip_level1_vs_level9_different_sizes(self):
        data = _make_moderate_entropy_data(100_000)
        handler_low = CompressionHandler(algorithm="gzip", level=1)
        handler_high = CompressionHandler(algorithm="gzip", level=9)
        result_low = handler_low.compress(data)
        result_high = handler_high.compress(data)
        # Level 9 should produce smaller (or equal) output than level 1
        assert result_high.compressed_size <= result_low.compressed_size
        # They should actually differ for moderate-entropy data
        assert result_low.compressed_size != result_high.compressed_size

    def test_zlib_level1_vs_level9_different_sizes(self):
        data = _make_moderate_entropy_data(100_000)
        handler_low = CompressionHandler(algorithm="zlib", level=1)
        handler_high = CompressionHandler(algorithm="zlib", level=9)
        result_low = handler_low.compress(data)
        result_high = handler_high.compress(data)
        assert result_high.compressed_size <= result_low.compressed_size
        assert result_low.compressed_size != result_high.compressed_size


# ── Bypass below threshold ──────────────────────────────────────────

class TestBypassThreshold:
    def test_data_below_threshold_returns_raw(self):
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=256)
        small_data = b"short log line"
        assert len(small_data) < 256
        result = handler.compress(small_data)
        assert result.data == small_data
        assert result.algorithm == "none"
        assert result.ratio == 1.0
        assert result.time_ms == 0.0
        assert result.level == 0

    def test_data_at_exact_threshold_is_compressed(self):
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=256)
        exact_data = b"x" * 256
        result = handler.compress(exact_data)
        assert result.algorithm == "gzip"

    def test_data_above_threshold_is_compressed(self):
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=256)
        big_data = b"y" * 1024
        result = handler.compress(big_data)
        assert result.algorithm == "gzip"

    def test_should_compress_returns_false_below_threshold(self):
        handler = CompressionHandler(algorithm="gzip", bypass_threshold=500)
        assert handler.should_compress(499) is False

    def test_should_compress_returns_true_at_threshold(self):
        handler = CompressionHandler(algorithm="gzip", bypass_threshold=500)
        assert handler.should_compress(500) is True


# ── Disabled returns raw ────────────────────────────────────────────

class TestDisabled:
    def test_disabled_returns_raw_data(self):
        handler = CompressionHandler(algorithm="gzip", level=6, enabled=False)
        data = _make_log_data(100)
        result = handler.compress(data)
        assert result.data == data
        assert result.algorithm == "none"
        assert result.ratio == 1.0
        assert result.level == 0

    def test_should_compress_returns_false_when_disabled(self):
        handler = CompressionHandler(algorithm="gzip", enabled=False)
        assert handler.should_compress(99999) is False

    def test_enabled_property(self):
        handler_on = CompressionHandler(enabled=True)
        handler_off = CompressionHandler(enabled=False)
        assert handler_on.enabled is True
        assert handler_off.enabled is False


# ── Algorithm "none" ────────────────────────────────────────────────

class TestAlgorithmNone:
    def test_algorithm_none_returns_raw_data(self):
        handler = CompressionHandler(algorithm="none", bypass_threshold=0)
        data = b"this should not be compressed"
        result = handler.compress(data)
        assert result.data == data
        assert result.algorithm == "none"
        assert result.ratio == 1.0

    def test_decompress_none_returns_data_unchanged(self):
        handler = CompressionHandler(algorithm="none")
        data = b"raw data passthrough"
        assert handler.decompress(data, "none") == data


# ── Invalid algorithm ───────────────────────────────────────────────

class TestInvalidAlgorithm:
    def test_constructor_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            CompressionHandler(algorithm="lz4")

    def test_constructor_raises_for_unknown_algo(self):
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            CompressionHandler(algorithm="brotli")


# ── Decompress with wrong algorithm raises error ────────────────────

class TestDecompressErrors:
    def test_gzip_data_decompressed_as_zlib_raises(self):
        handler = CompressionHandler(algorithm="gzip", level=6)
        data = _make_log_data(50)
        result = handler.compress(data)
        with pytest.raises(Exception):
            handler.decompress(result.data, "zlib")

    def test_zlib_data_decompressed_as_gzip_raises(self):
        handler = CompressionHandler(algorithm="zlib", level=6)
        data = _make_log_data(50)
        result = handler.compress(data)
        with pytest.raises(Exception):
            handler.decompress(result.data, "gzip")

    def test_decompress_unsupported_algorithm_raises(self):
        handler = CompressionHandler(algorithm="gzip")
        with pytest.raises(ValueError, match="Unsupported algorithm"):
            handler.decompress(b"data", "lz4")


# ── Thread-safe level setter ───────────────────────────────────────

class TestThreadSafeLevel:
    def test_level_clamped_to_min_1(self):
        handler = CompressionHandler(level=5)
        handler.level = 0
        assert handler.level == 1

    def test_level_clamped_to_max_9(self):
        handler = CompressionHandler(level=5)
        handler.level = 15
        assert handler.level == 9

    def test_level_negative_clamped_to_1(self):
        handler = CompressionHandler(level=5)
        handler.level = -10
        assert handler.level == 1

    def test_level_within_range_accepted(self):
        handler = CompressionHandler(level=5)
        handler.level = 7
        assert handler.level == 7

    def test_concurrent_level_updates_are_safe(self):
        """Many threads updating the level concurrently should not corrupt state."""
        handler = CompressionHandler(level=5)
        errors = []

        def updater(target_level):
            try:
                for _ in range(500):
                    handler.level = target_level
                    current = handler.level
                    if not (1 <= current <= 9):
                        errors.append(f"Level out of range: {current}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=updater, args=(lvl,)) for lvl in range(1, 10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread-safety errors: {errors}"
        assert 1 <= handler.level <= 9


# ── CompressionResult fields ───────────────────────────────────────

class TestCompressionResultFields:
    def test_all_fields_populated_after_gzip_compress(self):
        handler = CompressionHandler(algorithm="gzip", level=4)
        data = _make_log_data(100)
        result = handler.compress(data)

        assert isinstance(result, CompressionResult)
        assert isinstance(result.data, bytes)
        assert result.original_size == len(data)
        assert result.compressed_size == len(result.data)
        assert result.ratio == result.original_size / result.compressed_size
        assert result.time_ms >= 0.0
        assert result.algorithm == "gzip"
        assert result.level == 4

    def test_all_fields_populated_after_zlib_compress(self):
        handler = CompressionHandler(algorithm="zlib", level=3)
        data = _make_log_data(100)
        result = handler.compress(data)

        assert isinstance(result, CompressionResult)
        assert result.original_size == len(data)
        assert result.compressed_size == len(result.data)
        assert result.algorithm == "zlib"
        assert result.level == 3

    def test_bypassed_result_fields(self):
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=9999)
        data = b"tiny"
        result = handler.compress(data)

        assert result.data == data
        assert result.original_size == len(data)
        assert result.compressed_size == len(data)
        assert result.ratio == 1.0
        assert result.time_ms == 0.0
        assert result.algorithm == "none"
        assert result.level == 0


# ── Graceful failure on bad input ───────────────────────────────────

class TestGracefulFailure:
    def test_compress_empty_bytes(self):
        """Compressing empty bytes should not crash (falls below default threshold)."""
        handler = CompressionHandler(algorithm="gzip", level=6)
        result = handler.compress(b"")
        assert result.data == b""
        assert result.algorithm == "none"

    def test_compress_empty_bytes_with_zero_threshold(self):
        """Even with threshold=0, compressing empty bytes should work."""
        handler = CompressionHandler(algorithm="gzip", level=6, bypass_threshold=0)
        result = handler.compress(b"")
        # Empty bytes is size 0; 0 >= 0 is True so compression is attempted
        # gzip.compress(b"") is valid, should succeed
        assert isinstance(result.data, bytes)

    def test_decompress_garbage_data_raises(self):
        """Decompressing garbage should raise, not return silently."""
        handler = CompressionHandler(algorithm="gzip")
        with pytest.raises(Exception):
            handler.decompress(b"this is not gzip data", "gzip")

    def test_decompress_garbage_zlib_raises(self):
        handler = CompressionHandler(algorithm="zlib")
        with pytest.raises(Exception):
            handler.decompress(b"this is not zlib data", "zlib")


# ── Property accessors ──────────────────────────────────────────────

class TestPropertyAccessors:
    def test_algorithm_property(self):
        handler = CompressionHandler(algorithm="zlib")
        assert handler.algorithm == "zlib"

    def test_initial_level_property(self):
        handler = CompressionHandler(level=3)
        assert handler.level == 3

    def test_algorithm_stored_lowercase(self):
        handler = CompressionHandler(algorithm="GZIP")
        assert handler.algorithm == "gzip"
