"""Value serialization + zstd compression codec for cache tiers.

Cache values are serialized to compact UTF-8 JSON. Repetitive payloads
(notably time-series blobs, §3 Feature D) are additionally zstd-compressed,
which gives roughly 3-5x size reduction at very low CPU cost.

The on-the-wire format produced by :func:`encode_value` is a single leading
flag byte followed by the body::

    [flag:1 byte][body:N bytes]

    flag == FLAG_PLAIN (0x00)  ->  body is UTF-8 JSON
    flag == FLAG_ZSTD  (0x01)  ->  body is zstd-compressed UTF-8 JSON

:func:`decode_value` reads the flag from byte 0 and reverses the pipeline, so
callers never have to remember whether a stored blob was compressed.
"""
from __future__ import annotations

import json
from typing import Any

import zstandard

# Magic flag byte prepended to every encoded value so decode can self-detect
# the on-disk representation without out-of-band metadata.
FLAG_PLAIN = 0  # body is plain UTF-8 JSON
FLAG_ZSTD = 1   # body is zstd-compressed UTF-8 JSON


def dumps_json(obj: Any) -> bytes:
    """Serialize ``obj`` to compact UTF-8 JSON bytes.

    Uses the tightest separators (no spaces) and falls back to ``str`` for
    types JSON cannot natively encode (e.g. ``datetime``, ``Decimal``).
    """
    return json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")


def loads_json(b: bytes) -> Any:
    """Deserialize UTF-8 JSON ``bytes`` back into a Python object."""
    return json.loads(b)


def compress(data: bytes, level: int = 3) -> bytes:
    """zstd-compress raw ``bytes`` at the given level (default 3)."""
    return zstandard.ZstdCompressor(level=level).compress(data)


# Private alias so encode_value can call the module-level compressor even
# though its keyword-only ``compress`` parameter shadows the name in scope.
_compress = compress


def decompress(data: bytes) -> bytes:
    """Reverse :func:`compress`, returning the original raw ``bytes``."""
    return zstandard.ZstdDecompressor().decompress(data)


def encode_value(obj: Any, *, compress: bool = False, level: int = 3) -> bytes:
    """Encode ``obj`` into a flag-prefixed, optionally-compressed blob.

    Args:
        obj: any JSON-serializable object (or one stringifiable via ``str``).
        compress: when ``True``, zstd-compress the JSON body and tag it
            ``FLAG_ZSTD``; otherwise store plain JSON tagged ``FLAG_PLAIN``.
        level: zstd compression level used when ``compress`` is ``True``.

    Returns:
        ``bytes([flag]) + body`` — ready to store in L2 (binary-safe).
    """
    body = dumps_json(obj)
    if compress:
        body = _compress(body, level=level)
        flag = FLAG_ZSTD
    else:
        flag = FLAG_PLAIN
    return bytes([flag]) + body


def decode_value(b: bytes) -> Any:
    """Decode a blob produced by :func:`encode_value` back into an object.

    Reads the leading flag byte to decide whether the body must be
    decompressed before JSON parsing. Raises :class:`ValueError` on an
    unrecognized flag.
    """
    if not b:
        raise ValueError("cannot decode empty value")
    flag = b[0]
    body = b[1:]
    if flag == FLAG_PLAIN:
        return loads_json(body)
    if flag == FLAG_ZSTD:
        return loads_json(decompress(body))
    raise ValueError(f"unknown compression flag byte: {flag:#04x}")
