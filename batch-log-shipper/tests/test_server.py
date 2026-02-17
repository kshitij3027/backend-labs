"""Tests for the UDP log server."""

import socket
import threading
import time

import pytest

from src.config import ServerConfig
from src.server import UDPLogServer
from src.serializer import serialize_batch
from src.models import create_log_entry, entry_to_dict


@pytest.fixture
def server_pair():
    """Start a server on port 0 (ephemeral) and yield (server, address)."""
    config = ServerConfig(host="127.0.0.1", port=0, buffer_size=65535)
    shutdown = threading.Event()
    server = UDPLogServer(config, shutdown)
    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    # Wait for server to bind
    for _ in range(50):
        if server.server_address is not None:
            break
        time.sleep(0.05)
    yield server, server.server_address
    server.stop()
    thread.join(timeout=5)


def _make_entries(count: int) -> list[dict]:
    """Create a list of log entry dicts."""
    return [
        entry_to_dict(create_log_entry("INFO", f"test message {i}"))
        for i in range(count)
    ]


def _send_udp(data: bytes, address: tuple) -> None:
    """Send a UDP datagram to the given address."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.sendto(data, address)
    finally:
        sock.close()


class TestUDPLogServer:
    def test_receive_uncompressed_batch(self, server_pair):
        server, address = server_pair
        entries = _make_entries(3)
        data = serialize_batch(entries, compress=False)
        _send_udp(data, address)
        time.sleep(0.5)
        assert server.received_count == 3

    def test_receive_compressed_batch(self, server_pair):
        server, address = server_pair
        entries = _make_entries(5)
        data = serialize_batch(entries, compress=True)
        _send_udp(data, address)
        time.sleep(0.5)
        assert server.received_count == 5

    def test_ignore_invalid_data(self, server_pair):
        server, address = server_pair
        _send_udp(b"\xde\xad\xbe\xef", address)
        time.sleep(0.5)
        assert server.received_count == 0

    def test_shutdown_stops_server(self, server_pair):
        server, address = server_pair
        server.stop()
        # The server thread should exit promptly after stop()
        # Verify by checking that the shutdown event is set
        assert server._shutdown.is_set()

    def test_multiple_batches(self, server_pair):
        server, address = server_pair
        batch_sizes = [2, 4, 3]
        for size in batch_sizes:
            entries = _make_entries(size)
            data = serialize_batch(entries, compress=True)
            _send_udp(data, address)
        time.sleep(1.0)
        assert server.batch_count == 3
        assert server.received_count == 9
