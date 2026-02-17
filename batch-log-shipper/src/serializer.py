"""Batch serializer — JSON serialization with optional zlib compression."""

import json
import zlib

# 2-byte magic header to identify compressed data
MAGIC_HEADER = b"\xcb\xf2"

# Flags byte indicating compression
FLAG_COMPRESSED = 0x01


def serialize_batch(entries: list[dict], compress: bool = True) -> bytes:
    """Serialize a list of log-entry dicts to bytes.

    When *compress* is True the payload is zlib-compressed and prefixed with a
    3-byte header (2-byte magic + 1-byte flags).  When False the raw UTF-8
    JSON bytes are returned with no header.
    """
    payload = json.dumps(entries).encode("utf-8")

    if compress:
        compressed = zlib.compress(payload)
        header = MAGIC_HEADER + bytes([FLAG_COMPRESSED])
        return header + compressed

    return payload


def deserialize_batch(data: bytes) -> list[dict]:
    """Deserialize bytes produced by *serialize_batch* back to a list of dicts.

    Automatically detects whether the data is compressed by checking for the
    magic header.
    """
    if data[:2] == MAGIC_HEADER:
        # flags byte is at index 2; remaining data starts at index 3
        _flags = data[2]
        payload = zlib.decompress(data[3:])
    else:
        payload = data

    return json.loads(payload)
