"""In-process integration tests — full flow from generation through TCP to server."""

import threading
import time

import pytest

from src.config import ClientConfig
from src.tcp_server import TCPLogReceiver
from src.metrics import ReceiverMetrics
from src.log_shipper import LogShipper
from src.log_generator import generate_log_entry


def _setup_server_and_shipper(
    algorithm="gzip", enabled=True, bypass=256, batch_size=50
):
    """Start an ephemeral server and a connected LogShipper.

    Uses *separate* shutdown events for the server and the shipper so that
    stopping the shipper's batch buffer (which sets its shutdown event)
    does not immediately tear down the server.
    """
    server_shutdown = threading.Event()
    shipper_shutdown = threading.Event()

    recv_metrics = ReceiverMetrics()
    server = TCPLogReceiver("127.0.0.1", 0, server_shutdown, recv_metrics)

    # Start server in background thread
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(0.3)  # Wait for bind

    host, port = server.server_address
    config = ClientConfig(
        server_host=host,
        server_port=port,
        batch_size=batch_size,
        batch_interval=1.0,
        compression_enabled=enabled,
        compression_algorithm=algorithm,
        bypass_threshold=bypass,
    )

    shipper = LogShipper(config, shipper_shutdown)
    assert shipper.start(), "Shipper failed to connect to server"

    return server, shipper, recv_metrics, server_shutdown, shipper_shutdown


def _ship_entries(shipper, count):
    """Generate and ship *count* log entries."""
    for _ in range(count):
        shipper.ship(generate_log_entry())


def _teardown(shipper, server_shutdown, settle_time=1.0):
    """Stop the shipper, wait for data to settle, then stop the server."""
    shipper.stop()
    # Give the server time to process any in-flight data
    time.sleep(settle_time)
    server_shutdown.set()


# ── Tests ────────────────────────────────────────────────────────────────


class TestGzipIntegration:
    """100 logs with gzip compression -> server receives all 100."""

    def test_100_logs_gzip(self):
        server, shipper, recv_metrics, srv_shutdown, _ = _setup_server_and_shipper(
            algorithm="gzip", enabled=True
        )

        _ship_entries(shipper, 100)
        _teardown(shipper, srv_shutdown)

        snap = recv_metrics.snapshot()
        assert snap["logs_received"] == 100, f"Expected 100, got {snap['logs_received']}"


class TestZlibIntegration:
    """100 logs with zlib compression -> server receives all 100."""

    def test_100_logs_zlib(self):
        server, shipper, recv_metrics, srv_shutdown, _ = _setup_server_and_shipper(
            algorithm="zlib", enabled=True
        )

        _ship_entries(shipper, 100)
        _teardown(shipper, srv_shutdown)

        snap = recv_metrics.snapshot()
        assert snap["logs_received"] == 100, f"Expected 100, got {snap['logs_received']}"


class TestUncompressedIntegration:
    """100 logs with compression disabled -> server receives all 100."""

    def test_100_logs_uncompressed(self):
        server, shipper, recv_metrics, srv_shutdown, _ = _setup_server_and_shipper(
            algorithm="gzip", enabled=False
        )

        _ship_entries(shipper, 100)
        _teardown(shipper, srv_shutdown)

        snap = recv_metrics.snapshot()
        assert snap["logs_received"] == 100, f"Expected 100, got {snap['logs_received']}"


class TestMetricsAgreement:
    """Shipper logs_sent must equal server logs_received."""

    def test_shipper_and_receiver_agree(self):
        server, shipper, recv_metrics, srv_shutdown, _ = _setup_server_and_shipper(
            algorithm="gzip", enabled=True
        )

        _ship_entries(shipper, 100)
        _teardown(shipper, srv_shutdown)

        shipper_snap = shipper.metrics.snapshot()
        recv_snap = recv_metrics.snapshot()

        assert shipper_snap["logs_sent"] == recv_snap["logs_received"], (
            f"Shipper sent {shipper_snap['logs_sent']} but "
            f"receiver got {recv_snap['logs_received']}"
        )
        assert shipper_snap["logs_sent"] == 100


class TestBypassThreshold:
    """With a very high bypass threshold, data arrives uncompressed so
    bytes_compressed == bytes_decompressed in ReceiverMetrics."""

    def test_bypass_sends_uncompressed(self):
        server, shipper, recv_metrics, srv_shutdown, _ = _setup_server_and_shipper(
            algorithm="gzip",
            enabled=True,
            bypass=999999,  # higher than any batch payload
        )

        _ship_entries(shipper, 100)
        _teardown(shipper, srv_shutdown)

        snap = recv_metrics.snapshot()
        assert snap["logs_received"] == 100, f"Expected 100, got {snap['logs_received']}"
        # When uncompressed, compressed_size == decompressed_size on the wire
        assert snap["bytes_compressed"] == snap["bytes_decompressed"], (
            f"Expected equal sizes, got compressed={snap['bytes_compressed']} "
            f"decompressed={snap['bytes_decompressed']}"
        )
