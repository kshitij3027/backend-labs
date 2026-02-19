"""4-byte length-prefixed wire framing for TLS log transmission."""

import struct


def encode_frame(payload: bytes) -> bytes:
    """Prepend a 4-byte big-endian length header to payload."""
    header = struct.pack("!I", len(payload))
    return header + payload


def decode_frame_header(header_bytes: bytes) -> int:
    """Parse 4-byte big-endian header to get payload length."""
    if len(header_bytes) != 4:
        raise ValueError(f"Header must be exactly 4 bytes, got {len(header_bytes)}")
    return struct.unpack("!I", header_bytes)[0]


def recv_exact(sock, num_bytes: int) -> bytes:
    """Receive exactly num_bytes from socket, raising on disconnect."""
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:
            raise ConnectionError("Connection closed while receiving data")
        data += chunk
    return data
