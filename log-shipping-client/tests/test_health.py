"""Tests for the health monitor module."""

import socket
import threading
import time

from src.health import HealthMonitor


def _start_listener():
    """Start a TCP listener on a random port. Returns (sock, host, port)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(5)
    host, port = sock.getsockname()
    return sock, host, port


class TestHealthMonitorHealthy:
    def test_healthy_server(self):
        """Server is running -> is_healthy should be True."""
        listener, host, port = _start_listener()
        shutdown = threading.Event()
        monitor = HealthMonitor(host, port, shutdown, interval=0.1)
        try:
            # Accept connections in background so probes succeed
            def accept_loop():
                listener.settimeout(0.5)
                while not shutdown.is_set():
                    try:
                        conn, _ = listener.accept()
                        conn.close()
                    except (socket.timeout, OSError):
                        continue

            t = threading.Thread(target=accept_loop, daemon=True)
            t.start()

            monitor.start()
            assert monitor.wait_for_healthy(timeout=2.0) is True
            assert monitor.is_healthy is True
        finally:
            shutdown.set()
            listener.close()


class TestHealthMonitorUnhealthy:
    def test_no_server(self):
        """No server running -> is_healthy should be False."""
        shutdown = threading.Event()
        # Use a port that nothing is listening on
        monitor = HealthMonitor("127.0.0.1", 1, shutdown, interval=0.1)
        try:
            monitor.start()
            time.sleep(0.5)
            assert monitor.is_healthy is False
        finally:
            shutdown.set()


class TestHealthMonitorTransition:
    def test_unhealthy_to_healthy(self):
        """Server starts after monitor -> should transition to healthy."""
        shutdown = threading.Event()
        # Find a free port
        tmp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tmp.bind(("127.0.0.1", 0))
        _, port = tmp.getsockname()
        tmp.close()

        monitor = HealthMonitor("127.0.0.1", port, shutdown, interval=0.1)
        try:
            monitor.start()
            time.sleep(0.3)
            assert monitor.is_healthy is False

            # Now start the server
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))
            listener.listen(5)

            def accept_loop():
                listener.settimeout(0.5)
                while not shutdown.is_set():
                    try:
                        conn, _ = listener.accept()
                        conn.close()
                    except (socket.timeout, OSError):
                        continue

            t = threading.Thread(target=accept_loop, daemon=True)
            t.start()

            assert monitor.wait_for_healthy(timeout=2.0) is True
            assert monitor.is_healthy is True
        finally:
            shutdown.set()
            try:
                listener.close()
            except Exception:
                pass


class TestWaitForHealthy:
    def test_wait_timeout(self):
        """wait_for_healthy returns False on timeout when no server."""
        shutdown = threading.Event()
        monitor = HealthMonitor("127.0.0.1", 1, shutdown, interval=0.1)
        try:
            monitor.start()
            result = monitor.wait_for_healthy(timeout=0.5)
            assert result is False
        finally:
            shutdown.set()
