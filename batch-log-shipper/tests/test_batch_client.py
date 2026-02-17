"""Tests for the BatchLogClient orchestrator."""

import socket
import threading
import time

import pytest

from src.config import ClientConfig
from src.batch_client import BatchLogClient
from src.serializer import deserialize_batch


@pytest.fixture
def udp_receiver():
    """Start a real UDP socket on an ephemeral port to receive batches."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(5.0)
    _, port = sock.getsockname()
    yield sock, port
    sock.close()


def _make_config(port: int, **overrides) -> ClientConfig:
    """Build a ClientConfig aimed at the test receiver."""
    defaults = {
        "target_host": "127.0.0.1",
        "target_port": port,
        "batch_size": 10,
        "flush_interval": 30.0,
        "compress": True,
        "max_retries": 0,
        "logs_per_second": 5,
        "run_time": 10,
    }
    defaults.update(overrides)
    return ClientConfig(**defaults)


class TestBatchLogClient:
    """Integration tests that verify the full client pipeline end-to-end
    using a real UDP socket as the receiver."""

    def test_add_logs_flush_sends_batch(self, udp_receiver):
        """Adding exactly batch_size logs should trigger a size-based flush."""
        sock, port = udp_receiver
        shutdown = threading.Event()
        config = _make_config(port, batch_size=3, flush_interval=30.0)
        client = BatchLogClient(config, shutdown)

        try:
            client.add_log("INFO", "msg-1")
            client.add_log("INFO", "msg-2")
            client.add_log("INFO", "msg-3")

            data, _ = sock.recvfrom(65535)
            entries = deserialize_batch(data)

            assert len(entries) == 3
            assert entries[0]["message"] == "msg-1"
            assert entries[1]["message"] == "msg-2"
            assert entries[2]["message"] == "msg-3"
        finally:
            client.stop()

    def test_timer_flush(self, udp_receiver):
        """A single log below batch_size should flush after flush_interval."""
        sock, port = udp_receiver
        shutdown = threading.Event()
        config = _make_config(port, batch_size=100, flush_interval=0.5)
        client = BatchLogClient(config, shutdown)

        try:
            client.add_log("WARNING", "timer-test")

            # The timer thread checks every ~1s, so wait up to 5s
            data, _ = sock.recvfrom(65535)
            entries = deserialize_batch(data)

            assert len(entries) == 1
            assert entries[0]["message"] == "timer-test"
        finally:
            client.stop()

    def test_shutdown_flush(self, udp_receiver):
        """Stopping the client should flush any remaining buffered entries."""
        sock, port = udp_receiver
        shutdown = threading.Event()
        config = _make_config(port, batch_size=100, flush_interval=30.0)
        client = BatchLogClient(config, shutdown)

        client.add_log("ERROR", "shutdown-1")
        client.add_log("ERROR", "shutdown-2")

        # Stop triggers final flush
        client.stop()

        data, _ = sock.recvfrom(65535)
        entries = deserialize_batch(data)

        assert len(entries) == 2
        assert entries[0]["message"] == "shutdown-1"
        assert entries[1]["message"] == "shutdown-2"

    def test_dynamic_batch_size(self, udp_receiver):
        """Lowering batch_size at runtime should trigger a flush if the
        buffer already meets the new threshold."""
        sock, port = udp_receiver
        shutdown = threading.Event()
        config = _make_config(port, batch_size=10, flush_interval=30.0)
        client = BatchLogClient(config, shutdown)

        try:
            client.add_log("INFO", "dyn-1")
            client.add_log("INFO", "dyn-2")
            client.add_log("INFO", "dyn-3")

            # Lowering batch_size to 3 should trigger an immediate flush
            # because the buffer already has 3 entries.
            client.batch_size = 3

            data, _ = sock.recvfrom(65535)
            entries = deserialize_batch(data)

            assert len(entries) == 3
        finally:
            client.stop()

    def test_metrics_recorded(self, udp_receiver):
        """After flushing a batch, the metrics collector should record it."""
        sock, port = udp_receiver
        shutdown = threading.Event()
        config = _make_config(port, batch_size=3, flush_interval=30.0)
        client = BatchLogClient(config, shutdown)

        try:
            client.add_log("INFO", "metric-1")
            client.add_log("INFO", "metric-2")
            client.add_log("INFO", "metric-3")

            # Wait for the flush callback to complete
            data, _ = sock.recvfrom(65535)

            # Give the metrics recording a moment to finish
            time.sleep(0.1)

            snapshot = client.metrics.snapshot()
            assert snapshot["batches_sent"] >= 1
            assert snapshot["total_entries"] >= 3
            assert snapshot["total_bytes"] > 0
        finally:
            client.stop()
