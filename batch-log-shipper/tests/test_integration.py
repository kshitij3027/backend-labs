"""Integration tests — full client-to-server pipeline."""

import socket
import threading
import time

import pytest

from src.config import ServerConfig, ClientConfig
from src.server import UDPLogServer
from src.batch_client import BatchLogClient
from src.serializer import deserialize_batch


@pytest.fixture
def server_and_port():
    """Start a real UDPLogServer on an ephemeral port, return (server, port)."""
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
    assert server.server_address is not None, "Server failed to bind"
    yield server, server.server_address[1]
    server.stop()
    thread.join(timeout=5)


def _make_client(port, batch_size=10, flush_interval=60.0, compress=True):
    """Helper to create a BatchLogClient pointing at the test server."""
    config = ClientConfig(
        target_host="127.0.0.1",
        target_port=port,
        batch_size=batch_size,
        flush_interval=flush_interval,
        compress=compress,
    )
    shutdown = threading.Event()
    return BatchLogClient(config, shutdown)


def _wait_for_count(server, expected, timeout=3.0):
    """Poll until server.received_count reaches *expected* or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.received_count >= expected:
            return True
        time.sleep(0.05)
    return server.received_count >= expected


class TestFullPipeline:
    """End-to-end integration tests exercising the real UDP path."""

    def test_full_pipeline_50_logs(self, server_and_port):
        """Send 50 logs; expect 5 batches of 10 to arrive at the server."""
        server, port = server_and_port
        client = _make_client(port, batch_size=10, flush_interval=60.0)

        for i in range(50):
            client.add_log("INFO", f"integration-test-log-{i}")

        assert _wait_for_count(server, 50), (
            f"Expected 50 logs, got {server.received_count}"
        )

        client.stop()

        snap = client.metrics.snapshot()
        assert snap["batches_sent"] == 5
        assert snap["total_entries"] == 50

    def test_partial_batch_timer_flush(self, server_and_port):
        """Logs below the batch threshold are flushed by the timer."""
        server, port = server_and_port
        client = _make_client(port, batch_size=100, flush_interval=0.5)

        for i in range(7):
            client.add_log("WARNING", f"timer-flush-log-{i}")

        # Wait long enough for at least one timer flush cycle
        assert _wait_for_count(server, 7, timeout=2.5), (
            f"Expected 7 logs via timer flush, got {server.received_count}"
        )

        client.stop()

    def test_shutdown_flushes_remaining(self, server_and_port):
        """Calling client.stop() flushes any remaining buffered logs."""
        server, port = server_and_port
        client = _make_client(port, batch_size=100, flush_interval=60.0)

        for i in range(5):
            client.add_log("ERROR", f"shutdown-flush-log-{i}")

        # Immediately stop — should flush the 5 remaining entries
        client.stop()

        assert _wait_for_count(server, 5, timeout=2.0), (
            f"Expected 5 logs after shutdown flush, got {server.received_count}"
        )

    def test_compressed_e2e(self, server_and_port):
        """Compressed batches are correctly deserialized by the server."""
        server, port = server_and_port
        client = _make_client(port, batch_size=5, flush_interval=60.0, compress=True)

        for i in range(5):
            client.add_log("DEBUG", f"compressed-log-{i}")

        assert _wait_for_count(server, 5, timeout=3.0), (
            f"Expected 5 compressed logs, got {server.received_count}"
        )

        client.stop()
