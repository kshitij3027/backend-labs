"""Tests for the basic log shipper."""

import threading
import time

from src.config import Config
from src.server import SimpleLogServer
from src.shipper import LogShipper


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


class TestLogShipperBatch:
    def test_ships_all_sample_lines(self, tmp_path):
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
            )
            shipper = LogShipper(config, shutdown)
            shipper.run()

            assert shipper.sent == 5
            assert shipper.failed == 0
            assert len(server.received) == 5
        finally:
            shutdown.set()

    def test_skips_unparseable_lines(self, tmp_path):
        server, host, port, shutdown = _start_test_server()
        try:
            log_file = tmp_path / "test.log"
            log_file.write_text(
                "2024-01-15 08:23:45 INFO Valid line\n"
                "garbage line\n"
                "2024-01-15 08:25:00 INFO Another valid\n"
            )
            config = Config(
                log_file=str(log_file),
                server_host=host,
                server_port=port,
                batch_mode=True,
            )
            shipper = LogShipper(config, shutdown)
            shipper.run()

            assert shipper.sent == 2
            assert len(server.received) == 2
        finally:
            shutdown.set()


class TestLogShipperContinuous:
    def test_ships_appended_lines(self, tmp_path):
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
            )
            shipper = LogShipper(config, shutdown)
            t = threading.Thread(target=shipper.run, daemon=True)
            t.start()

            time.sleep(0.2)

            with open(str(log_file), "a") as f:
                f.write("2024-01-15 08:23:45 INFO Appended one\n")
                f.write("2024-01-15 08:24:01 WARNING Appended two\n")
                f.write("2024-01-15 08:25:00 ERROR Appended three\n")
                f.flush()

            time.sleep(0.5)
            shutdown.set()
            t.join(timeout=3)

            assert shipper.sent == 3
            assert len(server.received) == 3
        finally:
            shutdown.set()


class TestLogShipperConnectionFailure:
    def test_no_crash_on_connection_failure(self, tmp_path):
        shutdown = threading.Event()
        log_file = tmp_path / "test.log"
        log_file.write_text("2024-01-15 08:23:45 INFO Some line\n")

        config = Config(
            log_file=str(log_file),
            server_host="127.0.0.1",
            server_port=1,
            batch_mode=True,
        )
        shipper = LogShipper(config, shutdown)
        shipper.run()

        assert shipper.sent == 0
