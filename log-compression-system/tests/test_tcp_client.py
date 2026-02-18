"""Tests for TCPClient — connect, send_frame, close, and backoff."""

import socket
import threading
import time

import pytest

from src.protocol import Algorithm, HEADER_SIZE, decode_frame_header, recv_exact
from src.tcp_client import TCPClient


def _start_echo_server(ready_event: threading.Event) -> tuple[socket.socket, int]:
    """Start a simple TCP server on an ephemeral port and signal when ready.
    Returns the server socket and the port it is bound to."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.settimeout(5.0)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    ready_event.set()
    return srv, port


class TestTCPClientConnect:
    """Tests for connect() and close()."""

    def test_connect_success(self):
        """Client connects to a real ephemeral-port server."""
        ready = threading.Event()
        srv, port = _start_echo_server(ready)
        ready.wait(timeout=2)

        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", port, shutdown)

        try:
            assert client.connect() is True
            assert client.connected is True
        finally:
            client.close()
            srv.close()

    def test_connect_failure_wrong_port(self):
        """Client fails to connect to a port with no listener."""
        shutdown = threading.Event()
        # Use a port that is almost certainly not listening
        client = TCPClient("127.0.0.1", 1, shutdown)
        assert client.connect() is False
        assert client.connected is False

    def test_close_sets_connected_false(self):
        """After close(), connected returns False."""
        ready = threading.Event()
        srv, port = _start_echo_server(ready)
        ready.wait(timeout=2)

        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", port, shutdown)

        try:
            client.connect()
            assert client.connected is True
            client.close()
            assert client.connected is False
        finally:
            srv.close()


class TestTCPClientSendFrame:
    """Tests for send_frame()."""

    def test_send_frame_decodable(self):
        """Data sent via send_frame can be decoded on the server side."""
        ready = threading.Event()
        srv, port = _start_echo_server(ready)
        ready.wait(timeout=2)

        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", port, shutdown)

        try:
            client.connect()
            conn, _ = srv.accept()
            conn.settimeout(5.0)

            payload = b"hello world"
            assert client.send_frame(payload, compressed=False, algorithm=Algorithm.NONE) is True

            # Read header + payload on server side
            header = recv_exact(conn, HEADER_SIZE)
            length, is_compressed, algo = decode_frame_header(header)
            received = recv_exact(conn, length)

            assert received == payload
            assert is_compressed is False
            assert algo == Algorithm.NONE

            conn.close()
        finally:
            client.close()
            srv.close()

    def test_send_frame_no_connection(self):
        """send_frame returns False when not connected."""
        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", 9999, shutdown)
        assert client.send_frame(b"data", False, Algorithm.NONE) is False


class TestTCPClientBackoff:
    """Tests for connect_with_backoff()."""

    def test_backoff_success_first_try(self):
        """connect_with_backoff succeeds on the first attempt."""
        ready = threading.Event()
        srv, port = _start_echo_server(ready)
        ready.wait(timeout=2)

        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", port, shutdown)

        try:
            assert client.connect_with_backoff(max_attempts=3) is True
            assert client.connected is True
        finally:
            client.close()
            srv.close()

    def test_backoff_exhausts_max_attempts(self):
        """connect_with_backoff returns False after exhausting attempts."""
        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", 1, shutdown)
        assert client.connect_with_backoff(max_attempts=2) is False
        assert client.connected is False

    def test_backoff_stops_on_shutdown(self):
        """connect_with_backoff returns False when shutdown is set."""
        shutdown = threading.Event()
        client = TCPClient("127.0.0.1", 1, shutdown)

        # Set shutdown after a short delay so the backoff loop exits
        def set_shutdown():
            time.sleep(0.3)
            shutdown.set()

        t = threading.Thread(target=set_shutdown, daemon=True)
        t.start()

        # max_attempts=0 means infinite, but shutdown should break it
        result = client.connect_with_backoff(max_attempts=0)
        assert result is False
        t.join(timeout=2)
