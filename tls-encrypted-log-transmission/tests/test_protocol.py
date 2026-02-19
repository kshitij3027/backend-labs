"""Tests for wire protocol framing."""

import struct
import pytest
from src.protocol import encode_frame, decode_frame_header, recv_exact


class TestEncodeFrame:
    def test_basic_encoding(self):
        payload = b"hello"
        frame = encode_frame(payload)
        assert len(frame) == 4 + 5
        length = struct.unpack("!I", frame[:4])[0]
        assert length == 5
        assert frame[4:] == b"hello"

    def test_empty_payload(self):
        frame = encode_frame(b"")
        length = struct.unpack("!I", frame[:4])[0]
        assert length == 0
        assert frame[4:] == b""

    def test_large_payload(self):
        payload = b"x" * 100000
        frame = encode_frame(payload)
        length = struct.unpack("!I", frame[:4])[0]
        assert length == 100000


class TestDecodeFrameHeader:
    def test_valid_header(self):
        header = struct.pack("!I", 42)
        assert decode_frame_header(header) == 42

    def test_invalid_header_length(self):
        with pytest.raises(ValueError, match="4 bytes"):
            decode_frame_header(b"\x00\x00")


class TestRecvExact:
    def test_recv_in_chunks(self):
        """Mock socket that delivers data in small chunks."""
        chunks = [b"he", b"ll", b"o"]

        class FakeSocket:
            def __init__(self):
                self.idx = 0
            def recv(self, n):
                if self.idx >= len(chunks):
                    return b""
                data = chunks[self.idx]
                self.idx += 1
                return data

        result = recv_exact(FakeSocket(), 5)
        assert result == b"hello"

    def test_recv_disconnect(self):
        class FakeSocket:
            def recv(self, n):
                return b""

        with pytest.raises(ConnectionError):
            recv_exact(FakeSocket(), 5)
