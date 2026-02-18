"""Wire protocol: 5-byte header (4-byte length + 1-byte flags) + payload.

Flags byte layout:
  Bit 0:   compressed (1 = yes, 0 = no)
  Bit 1-2: algorithm (00 = none, 01 = gzip, 10 = zlib)
  Bit 3-7: reserved

Examples:
  0x00 = uncompressed
  0x03 = gzip compressed (bits: 011)
  0x05 = zlib compressed (bits: 101)
"""

import struct
from enum import IntEnum


class Algorithm(IntEnum):
    NONE = 0
    GZIP = 1
    ZLIB = 2


HEADER_SIZE = 5
HEADER_FORMAT = "!IB"  # 4-byte uint32 big-endian + 1-byte uint8


def encode_frame(payload: bytes, compressed: bool, algorithm: Algorithm) -> bytes:
    """Encode a payload into a framed message: [length][flags][payload].

    Args:
        payload: The data bytes to send.
        compressed: Whether the payload is compressed.
        algorithm: The compression algorithm used.

    Returns:
        The framed bytes ready to send over TCP.
    """
    flags = 0
    if compressed:
        flags |= 0x01              # set bit 0
        flags |= (algorithm & 0x03) << 1  # set bits 1-2
    header = struct.pack(HEADER_FORMAT, len(payload), flags)
    return header + payload


def decode_frame_header(header: bytes) -> tuple[int, bool, Algorithm]:
    """Decode a 5-byte header into (payload_length, is_compressed, algorithm).

    Args:
        header: Exactly 5 bytes.

    Returns:
        Tuple of (payload_length, is_compressed, algorithm).

    Raises:
        ValueError: If header is not exactly 5 bytes.
    """
    if len(header) != HEADER_SIZE:
        raise ValueError(f"Header must be {HEADER_SIZE} bytes, got {len(header)}")

    payload_length, flags = struct.unpack(HEADER_FORMAT, header)
    is_compressed = bool(flags & 0x01)
    algo_bits = (flags >> 1) & 0x03
    algorithm = Algorithm(algo_bits)
    return payload_length, is_compressed, algorithm


def recv_exact(sock, n: int) -> bytes:
    """Read exactly n bytes from a socket.

    Args:
        sock: A connected socket.
        n: Number of bytes to read.

    Returns:
        Exactly n bytes.

    Raises:
        ConnectionError: If the connection is closed before n bytes are read.
    """
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed before all data received")
        data += chunk
    return data
