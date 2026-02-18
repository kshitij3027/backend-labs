"""Tests for src/protocol.py — wire protocol encode/decode, recv_exact."""

import struct
from unittest.mock import MagicMock

import pytest

from src.protocol import (
    Algorithm,
    HEADER_FORMAT,
    HEADER_SIZE,
    decode_frame_header,
    encode_frame,
    recv_exact,
)


# ── Algorithm enum ─────────────────────────────────────────────────


class TestAlgorithmEnum:
    def test_none_is_zero(self):
        assert Algorithm.NONE == 0

    def test_gzip_is_one(self):
        assert Algorithm.GZIP == 1

    def test_zlib_is_two(self):
        assert Algorithm.ZLIB == 2


# ── Header constants ──────────────────────────────────────────────


class TestHeaderConstants:
    def test_header_size_is_five(self):
        assert HEADER_SIZE == 5

    def test_header_format_packs_to_five_bytes(self):
        packed = struct.pack(HEADER_FORMAT, 0, 0)
        assert len(packed) == 5


# ── Encode/decode round-trip ──────────────────────────────────────


class TestRoundTrip:
    def test_uncompressed_round_trip(self):
        payload = b"hello world"
        frame = encode_frame(payload, compressed=False, algorithm=Algorithm.NONE)
        header = frame[:HEADER_SIZE]
        length, is_compressed, algo = decode_frame_header(header)
        assert length == len(payload)
        assert is_compressed is False
        assert algo == Algorithm.NONE
        assert frame[HEADER_SIZE:] == payload

    def test_gzip_compressed_round_trip(self):
        payload = b"compressed gzip data"
        frame = encode_frame(payload, compressed=True, algorithm=Algorithm.GZIP)
        header = frame[:HEADER_SIZE]
        length, is_compressed, algo = decode_frame_header(header)
        assert length == len(payload)
        assert is_compressed is True
        assert algo == Algorithm.GZIP
        assert frame[HEADER_SIZE:] == payload

    def test_zlib_compressed_round_trip(self):
        payload = b"compressed zlib data"
        frame = encode_frame(payload, compressed=True, algorithm=Algorithm.ZLIB)
        header = frame[:HEADER_SIZE]
        length, is_compressed, algo = decode_frame_header(header)
        assert length == len(payload)
        assert is_compressed is True
        assert algo == Algorithm.ZLIB
        assert frame[HEADER_SIZE:] == payload


# ── Flag bit patterns ────────────────────────────────────────────


class TestFlagBitPatterns:
    def test_uncompressed_flag_is_0x00(self):
        frame = encode_frame(b"data", compressed=False, algorithm=Algorithm.NONE)
        flags_byte = frame[4]  # 5th byte (index 4) is the flags byte
        assert flags_byte == 0x00

    def test_gzip_compressed_flag_is_0x03(self):
        frame = encode_frame(b"data", compressed=True, algorithm=Algorithm.GZIP)
        flags_byte = frame[4]
        assert flags_byte == 0x03

    def test_zlib_compressed_flag_is_0x05(self):
        frame = encode_frame(b"data", compressed=True, algorithm=Algorithm.ZLIB)
        flags_byte = frame[4]
        assert flags_byte == 0x05


# ── Payload length encoding ──────────────────────────────────────


class TestPayloadLengthEncoding:
    def test_empty_payload_length_is_zero(self):
        frame = encode_frame(b"", compressed=False, algorithm=Algorithm.NONE)
        length, _, _ = decode_frame_header(frame[:HEADER_SIZE])
        assert length == 0

    def test_known_payload_length(self):
        payload = b"exactly twenty chars"
        frame = encode_frame(payload, compressed=False, algorithm=Algorithm.NONE)
        length, _, _ = decode_frame_header(frame[:HEADER_SIZE])
        assert length == len(payload)

    def test_large_payload_1mb_length(self):
        payload = b"X" * (1024 * 1024)  # 1 MB
        frame = encode_frame(payload, compressed=False, algorithm=Algorithm.NONE)
        length, _, _ = decode_frame_header(frame[:HEADER_SIZE])
        assert length == 1024 * 1024


# ── decode_frame_header errors ───────────────────────────────────


class TestDecodeFrameHeaderErrors:
    def test_header_too_short_raises_value_error(self):
        with pytest.raises(ValueError, match="Header must be 5 bytes, got 4"):
            decode_frame_header(b"\x00\x00\x00\x00")

    def test_header_too_long_raises_value_error(self):
        with pytest.raises(ValueError, match="Header must be 5 bytes, got 6"):
            decode_frame_header(b"\x00\x00\x00\x00\x00\x00")

    def test_empty_header_raises_value_error(self):
        with pytest.raises(ValueError, match="Header must be 5 bytes, got 0"):
            decode_frame_header(b"")


# ── Header format ────────────────────────────────────────────────


class TestHeaderFormat:
    def test_encode_frame_starts_with_5_byte_header(self):
        payload = b"test payload"
        frame = encode_frame(payload, compressed=False, algorithm=Algorithm.NONE)
        assert len(frame) == HEADER_SIZE + len(payload)

    def test_header_followed_by_exact_payload(self):
        payload = b"my log line"
        frame = encode_frame(payload, compressed=True, algorithm=Algorithm.GZIP)
        assert frame[HEADER_SIZE:] == payload


# ── recv_exact with mocked socket ────────────────────────────────


class TestRecvExact:
    def test_assembles_from_small_chunks(self):
        """Socket returns data in 3-byte chunks; recv_exact assembles all."""
        sock = MagicMock()
        full_data = b"ABCDEFGHIJ"  # 10 bytes
        # Return 3 bytes at a time, then the remaining 1
        sock.recv.side_effect = [b"ABC", b"DEF", b"GHI", b"J"]
        result = recv_exact(sock, 10)
        assert result == full_data

    def test_connection_closed_raises_connection_error(self):
        """Socket returns empty bytes (connection closed) before all data received."""
        sock = MagicMock()
        sock.recv.side_effect = [b"AB", b""]
        with pytest.raises(ConnectionError, match="Connection closed"):
            recv_exact(sock, 10)

    def test_all_at_once(self):
        """Socket returns all requested bytes in a single recv call."""
        sock = MagicMock()
        sock.recv.return_value = b"HELLO"
        result = recv_exact(sock, 5)
        assert result == b"HELLO"
        sock.recv.assert_called_once_with(5)

    def test_single_byte_chunks(self):
        """Socket returns one byte at a time."""
        sock = MagicMock()
        sock.recv.side_effect = [bytes([b]) for b in b"ABCD"]
        result = recv_exact(sock, 4)
        assert result == b"ABCD"
        assert sock.recv.call_count == 4

    def test_immediate_close_raises_connection_error(self):
        """Socket returns empty bytes immediately on first recv."""
        sock = MagicMock()
        sock.recv.return_value = b""
        with pytest.raises(ConnectionError, match="Connection closed"):
            recv_exact(sock, 1)
