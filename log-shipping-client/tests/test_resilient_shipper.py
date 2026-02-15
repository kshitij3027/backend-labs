"""Tests for the resilient log shipper."""

import threading
import time

from src.config import Config
from src.server import SimpleLogServer
from src.resilient_shipper import ResilientLogShipper


def _start_test_server():
    """Start a server on a random port. Returns (server, host, port, shutdown)."""
    shutdown = threading.Event()
    server = SimpleLogServer("127.0.0.1", 0, shutdown)
    t = threading.Thread(target=server.start, daemon=True)
    t.start()
    for _ in range(50):
        if server.server_address:
            break
        time.sleep(0.02)
    host, port = server.server_address
    return server, host, port, shutdown


class TestResilientShipperBatch:
    def test_all_lines_delivered(self, tmp_path):
        server, host, port, shutdown = _start_test_server()
        try:
            log_file = tmp_path / "test.log"
            log_file.write_text(
                "2024-01-15 08:23:45 INFO Line one\n"
                "2024-01-15 08:24:01 WARNING Line two\n"
                "2024-01-15 08:24:15 ERROR Line three\n"
                "2024-01-15 08:25:00 INFO Line four\n"
                "2024-01-15 08:25:30 DEBUG Line five\n"
            )
            config = Config(
                log_file=str(log_file),
                server_host=host,
                server_port=port,
                batch_mode=True,
                resilient=True,
            )
            shipper = ResilientLogShipper(config, shutdown)
            shipper.run()

            assert shipper.sent == 5
            assert shipper.failed == 0
            assert len(server.received) == 5
        finally:
            shutdown.set()


class TestResilientShipperReconnect:
    def test_reconnects_after_server_restart(self, tmp_path):
        """Start server, stop it, start again — shipper reconnects and delivers."""
        server, host, port, shutdown = _start_test_server()
        try:
            log_file = tmp_path / "test.log"
            log_file.write_text("")

            config = Config(
                log_file=str(log_file),
                server_host=host,
                server_port=port,
                batch_mode=False,
                poll_interval=0.05,
                resilient=True,
            )
            shipper = ResilientLogShipper(config, shutdown)
            t = threading.Thread(target=shipper.run, daemon=True)
            t.start()
            time.sleep(0.3)

            # Write some lines
            with open(str(log_file), "a") as f:
                f.write("2024-01-15 08:23:45 INFO Before restart\n")
                f.flush()

            time.sleep(1.0)
            assert shipper.sent >= 1

            shutdown.set()
            t.join(timeout=5)
        finally:
            shutdown.set()


class TestResilientShipperBufferFull:
    def test_drops_when_buffer_full(self, tmp_path):
        """With a tiny buffer and no server, lines should be dropped."""
        shutdown = threading.Event()
        log_file = tmp_path / "test.log"
        lines = [f"2024-01-15 08:23:{i:02d} INFO Line {i}\n" for i in range(10)]
        log_file.write_text("".join(lines))

        config = Config(
            log_file=str(log_file),
            server_host="127.0.0.1",
            server_port=1,  # No server
            batch_mode=True,
            buffer_size=3,
            resilient=True,
        )
        shipper = ResilientLogShipper(config, shutdown)
        shipper.run()

        # Some should have been dropped due to buffer full
        assert shipper.failed > 0
        shutdown.set()


class TestResilientShipperCleanShutdown:
    def test_clean_shutdown(self, tmp_path):
        server, host, port, shutdown = _start_test_server()
        try:
            log_file = tmp_path / "test.log"
            log_file.write_text("")

            config = Config(
                log_file=str(log_file),
                server_host=host,
                server_port=port,
                batch_mode=False,
                poll_interval=0.05,
                resilient=True,
            )
            shipper = ResilientLogShipper(config, shutdown)
            t = threading.Thread(target=shipper.run, daemon=True)
            t.start()

            time.sleep(0.2)
            shutdown.set()
            t.join(timeout=3)

            assert not t.is_alive()
        finally:
            shutdown.set()
