"""Tests for the UDP log server."""

import json
import socket
import threading
import time

import pytest

from src.config import Config
from src.server import UDPLogServer


def _make_server(tmp_path, **overrides):
    """Create a server with port=0 (OS-assigned) and return (server, thread, shutdown_event)."""
    defaults = {
        "host": "127.0.0.1",
        "port": 0,
        "buffer_size": 65536,
        "log_dir": str(tmp_path / "logs"),
        "log_filename": "test.log",
        "flush_count": 100,
        "flush_timeout_sec": 5,
        "max_errors": 100,
    }
    defaults.update(overrides)
    config = Config(**defaults)
    shutdown_event = threading.Event()
    server = UDPLogServer(config, shutdown_event)

    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()

    for _ in range(50):
        if server.server_address is not None:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("Server failed to bind")

    return server, thread, shutdown_event


def _send_udp(host, port, payload: dict):
    """Send a single UDP datagram."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        data = json.dumps(payload).encode("utf-8")
        sock.sendto(data, (host, port))
    finally:
        sock.close()


class TestServerReceive:
    def test_receives_single_message(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            _send_udp(host, port, {"level": "INFO", "message": "hello"})
            time.sleep(0.2)
            assert server.received_count == 1
        finally:
            server.stop()
            thread.join(timeout=5)

    def test_receives_multiple_messages(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            for i in range(10):
                _send_udp(host, port, {"level": "INFO", "message": f"msg-{i}"})
            time.sleep(0.5)
            assert server.received_count == 10
        finally:
            server.stop()
            thread.join(timeout=5)

    def test_ignores_invalid_json(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(b"not json", (host, port))
            sock.close()
            time.sleep(0.2)
            assert server.received_count == 0
        finally:
            server.stop()
            thread.join(timeout=5)

    def test_shutdown_stops_server(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        server.stop()
        thread.join(timeout=5)
        assert not thread.is_alive()
