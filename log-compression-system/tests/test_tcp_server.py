"""Tests for TCPLogReceiver — frame decoding, decompression, multi-client."""

import gzip
import json
import socket
import threading
import time
import zlib

import pytest

from src.metrics import ReceiverMetrics
from src.protocol import Algorithm, encode_frame
from src.tcp_server import TCPLogReceiver


def _start_receiver(host="127.0.0.1", port=0, metrics=None):
    """Start a TCPLogReceiver in a background thread and return (receiver, thread).
    Waits until the server is bound and ready to accept connections."""
    shutdown = threading.Event()
    receiver = TCPLogReceiver(host, port, shutdown, metrics=metrics)
    t = threading.Thread(target=receiver.start, daemon=True)
    t.start()
    # Wait for the server to bind
    deadline = time.monotonic() + 3.0
    while receiver.server_address is None and time.monotonic() < deadline:
        time.sleep(0.05)
    assert receiver.server_address is not None, "Server failed to bind in time"
    return receiver, t


def _send_frame_to(host, port, payload, compressed, algorithm):
    """Connect, send one frame, and close."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    sock.connect((host, port))
    frame = encode_frame(payload, compressed, algorithm)
    sock.sendall(frame)
    # Give server time to process
    time.sleep(0.1)
    sock.close()


def _make_log_batch(count: int) -> list[dict]:
    """Create a batch of dummy log entries."""
    return [
        {"timestamp": "2024-01-01T00:00:00Z", "level": "INFO",
         "message": f"test log {i}", "service": "test", "metadata": {}}
        for i in range(count)
    ]


class TestTCPLogReceiverGzip:
    """Test receiving gzip-compressed frames."""

    def test_receive_gzip_compressed(self):
        metrics = ReceiverMetrics()
        receiver, t = _start_receiver(metrics=metrics)
        host, port = receiver.server_address

        try:
            batch = _make_log_batch(5)
            raw = json.dumps(batch).encode("utf-8")
            compressed = gzip.compress(raw)

            _send_frame_to(host, port, compressed, True, Algorithm.GZIP)

            # Allow time for server to process
            time.sleep(0.3)

            snap = metrics.snapshot()
            assert snap["logs_received"] == 5
            assert snap["batches_received"] == 1
            assert snap["bytes_compressed"] == len(compressed)
            assert snap["bytes_decompressed"] == len(raw)
        finally:
            receiver.stop()
            t.join(timeout=3)


class TestTCPLogReceiverZlib:
    """Test receiving zlib-compressed frames."""

    def test_receive_zlib_compressed(self):
        metrics = ReceiverMetrics()
        receiver, t = _start_receiver(metrics=metrics)
        host, port = receiver.server_address

        try:
            batch = _make_log_batch(3)
            raw = json.dumps(batch).encode("utf-8")
            compressed = zlib.compress(raw)

            _send_frame_to(host, port, compressed, True, Algorithm.ZLIB)

            time.sleep(0.3)

            snap = metrics.snapshot()
            assert snap["logs_received"] == 3
            assert snap["batches_received"] == 1
            assert snap["bytes_compressed"] == len(compressed)
            assert snap["bytes_decompressed"] == len(raw)
        finally:
            receiver.stop()
            t.join(timeout=3)


class TestTCPLogReceiverUncompressed:
    """Test receiving uncompressed frames."""

    def test_receive_uncompressed(self):
        metrics = ReceiverMetrics()
        receiver, t = _start_receiver(metrics=metrics)
        host, port = receiver.server_address

        try:
            batch = _make_log_batch(2)
            raw = json.dumps(batch).encode("utf-8")

            _send_frame_to(host, port, raw, False, Algorithm.NONE)

            time.sleep(0.3)

            snap = metrics.snapshot()
            assert snap["logs_received"] == 2
            assert snap["batches_received"] == 1
            # Uncompressed: compressed_size == decompressed_size == len(raw)
            assert snap["bytes_compressed"] == len(raw)
            assert snap["bytes_decompressed"] == len(raw)
        finally:
            receiver.stop()
            t.join(timeout=3)


class TestTCPLogReceiverMultiClient:
    """Test multiple concurrent clients."""

    def test_three_clients_concurrent(self):
        metrics = ReceiverMetrics()
        receiver, t = _start_receiver(metrics=metrics)
        host, port = receiver.server_address

        try:
            threads = []
            logs_per_client = 4
            num_clients = 3

            for _ in range(num_clients):
                batch = _make_log_batch(logs_per_client)
                raw = json.dumps(batch).encode("utf-8")
                compressed = gzip.compress(raw)

                ct = threading.Thread(
                    target=_send_frame_to,
                    args=(host, port, compressed, True, Algorithm.GZIP),
                    daemon=True,
                )
                ct.start()
                threads.append(ct)

            for ct in threads:
                ct.join(timeout=5)

            # Allow time for all processing
            time.sleep(0.5)

            snap = metrics.snapshot()
            assert snap["logs_received"] == num_clients * logs_per_client
            assert snap["batches_received"] == num_clients
        finally:
            receiver.stop()
            t.join(timeout=3)


class TestTCPLogReceiverShutdown:
    """Test server shutdown."""

    def test_stop_exits_accept_loop(self):
        """Calling stop() causes the server thread to exit."""
        receiver, t = _start_receiver()
        assert receiver.server_address is not None

        receiver.stop()
        t.join(timeout=3)
        assert not t.is_alive(), "Server thread did not exit after stop()"
