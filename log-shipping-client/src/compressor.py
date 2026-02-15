"""Gzip compression with length-prefixed framing for TCP transport."""

import gzip
import struct


def compress_payload(data: bytes) -> bytes:
    """Compress data with gzip and prepend a 4-byte big-endian length header.

    Frame format: [4-byte BE uint32 compressed_length][gzip compressed data]
    """
    compressed = gzip.compress(data)
    header = struct.pack("!I", len(compressed))
    return header + compressed


def decompress_frame(frame: bytes) -> bytes:
    """Read 4-byte length header, extract and decompress the gzip payload."""
    if len(frame) < 4:
        raise ValueError("Frame too short: need at least 4 bytes for header")

    length = struct.unpack("!I", frame[:4])[0]
    compressed = frame[4:4 + length]

    if len(compressed) < length:
        raise ValueError(
            f"Incomplete frame: expected {length} bytes, got {len(compressed)}"
        )

    return gzip.decompress(compressed)


def is_compressed(data: bytes) -> bool:
    """Detect whether a payload is a compressed frame.

    Compressed frames have a 4-byte BE length header followed by gzip data.
    Gzip data starts with magic bytes 0x1f 0x8b. We check for these after
    the length header. Plain NDJSON starts with '{' (0x7B).
    """
    if len(data) < 6:
        # Need at least 4-byte header + 2-byte gzip magic
        return data[0:1] != b"{" if data else False
    # Check gzip magic bytes at offset 4 (after length header)
    return data[4] == 0x1F and data[5] == 0x8B
