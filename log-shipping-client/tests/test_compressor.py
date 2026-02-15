"""Tests for the compressor module."""

import gzip
import struct

import pytest

from src.compressor import compress_payload, decompress_frame, is_compressed


class TestCompressPayload:
    def test_roundtrip(self):
        original = b'{"level":"INFO","message":"hello"}\n'
        frame = compress_payload(original)
        result = decompress_frame(frame)
        assert result == original

    def test_frame_format(self):
        data = b"test data"
        frame = compress_payload(data)
        # First 4 bytes are length header
        length = struct.unpack("!I", frame[:4])[0]
        # Remaining bytes should be valid gzip
        compressed = frame[4:]
        assert len(compressed) == length
        assert gzip.decompress(compressed) == data

    def test_multiple_lines(self):
        lines = b'{"level":"INFO","message":"one"}\n{"level":"ERROR","message":"two"}\n'
        frame = compress_payload(lines)
        result = decompress_frame(frame)
        assert result == lines


class TestDecompressFrame:
    def test_too_short(self):
        with pytest.raises(ValueError, match="too short"):
            decompress_frame(b"\x00\x01")

    def test_incomplete_frame(self):
        # Header says 1000 bytes but only 5 provided
        header = struct.pack("!I", 1000)
        with pytest.raises(ValueError, match="Incomplete"):
            decompress_frame(header + b"\x00" * 5)


class TestIsCompressed:
    def test_plain_ndjson(self):
        assert is_compressed(b'{"level":"INFO"}') is False

    def test_compressed_frame(self):
        frame = compress_payload(b"test")
        assert is_compressed(frame) is True

    def test_plain_text_not_json(self):
        # "not json" should not be detected as compressed (no gzip magic)
        assert is_compressed(b"not json\n") is False

    def test_short_data(self):
        assert is_compressed(b"{") is False
        assert is_compressed(b"") is False
