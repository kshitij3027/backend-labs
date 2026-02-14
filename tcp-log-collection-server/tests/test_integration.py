"""Integration tests — start a real TCP server and connect with real sockets."""

import json
import os
import socket
import tempfile
import threading
import time

import pytest

from src.config import Config
from src.server import TCPLogServer


def _make_server(tmp_path, **overrides):
    """Create a server with port=0 (OS-assigned) and return (server, thread, shutdown_event)."""
    defaults = {
        "host": "127.0.0.1",
        "port": 0,
        "buffer_size": 4096,
        "min_log_level": "INFO",
        "enable_log_persistence": True,
        "log_dir": str(tmp_path / "logs"),
        "log_filename": "test.log",
        "rate_limit_enabled": False,
        "rate_limit_max_requests": 100,
        "rate_limit_window_seconds": 60,
    }
    defaults.update(overrides)
    config = Config(**defaults)
    shutdown_event = threading.Event()
    server = TCPLogServer(config, shutdown_event)

    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()

    # Wait for server to bind
    for _ in range(50):
        if server.server_address is not None:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("Server failed to bind")

    return server, thread, shutdown_event


def _connect(host, port, retries=10, delay=0.1):
    """Connect to the server with retries."""
    for i in range(retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((host, port))
            return sock
        except ConnectionRefusedError:
            sock.close()
            time.sleep(delay)
    raise ConnectionRefusedError(f"Could not connect to {host}:{port} after {retries} retries")


def _send_and_recv(sock, payload: dict) -> dict:
    """Send an NDJSON message and receive the response."""
    data = json.dumps(payload) + "\n"
    sock.sendall(data.encode("utf-8"))

    response = b""
    while b"\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
    return json.loads(response.decode("utf-8").strip())


class TestBasicSendAndAck:
    def test_error_message_received(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"level": "ERROR", "message": "disk full"})
                assert resp["status"] == "ok"
                assert resp["message"] == "received"
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)


class TestFilteredMessage:
    def test_debug_below_info_threshold(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path, min_log_level="INFO")
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"level": "DEBUG", "message": "verbose"})
                assert resp["status"] == "ok"
                assert resp["message"] == "filtered"
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)


class TestInvalidJSON:
    def test_garbage_input(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                sock.sendall(b"this is not json\n")
                response = b""
                while b"\n" not in response:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk
                resp = json.loads(response.decode("utf-8").strip())
                assert resp["status"] == "error"
                assert "invalid JSON" in resp["message"]
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)


class TestMissingFields:
    def test_missing_message_field(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"level": "ERROR"})
                assert resp["status"] == "error"
                assert "missing" in resp["message"].lower()
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)

    def test_missing_level_field(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"message": "no level"})
                assert resp["status"] == "error"
                assert "missing" in resp["message"].lower()
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)


class TestPersistenceFileWritten:
    def test_file_contains_message(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"level": "ERROR", "message": "disk full"})
                assert resp["status"] == "ok"
            finally:
                sock.close()

            # Give a moment for flush
            time.sleep(0.1)

            log_path = str(tmp_path / "logs" / "test.log")
            assert os.path.isfile(log_path)
            content = open(log_path).read()
            assert "[ERROR]" in content
            assert "disk full" in content
        finally:
            server.stop()
            thread.join(timeout=5)


class TestMultipleClients:
    def test_five_concurrent_clients(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        results = [None] * 5

        def client_worker(idx):
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                resp = _send_and_recv(sock, {"level": "ERROR", "message": f"client-{idx}"})
                results[idx] = resp
            finally:
                sock.close()

        try:
            threads = [threading.Thread(target=client_worker, args=(i,)) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            for i, resp in enumerate(results):
                assert resp is not None, f"Client {i} got no response"
                assert resp["status"] == "ok"
                assert resp["message"] == "received"
        finally:
            server.stop()
            thread.join(timeout=5)


class TestMultipleMessagesSingleConnection:
    def test_ten_messages(self, tmp_path):
        server, thread, shutdown = _make_server(tmp_path)
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                for i in range(10):
                    resp = _send_and_recv(sock, {"level": "ERROR", "message": f"msg-{i}"})
                    assert resp["status"] == "ok"
                    assert resp["message"] == "received"
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)


class TestRateLimitKicksIn:
    def test_fourth_request_rejected(self, tmp_path):
        server, thread, shutdown = _make_server(
            tmp_path,
            rate_limit_enabled=True,
            rate_limit_max_requests=3,
            rate_limit_window_seconds=60,
        )
        try:
            host, port = server.server_address
            sock = _connect(host, port)
            try:
                for i in range(3):
                    resp = _send_and_recv(sock, {"level": "ERROR", "message": f"msg-{i}"})
                    assert resp["status"] == "ok", f"Request {i} should be ok"

                # 4th request should be rate-limited
                resp = _send_and_recv(sock, {"level": "ERROR", "message": "over limit"})
                assert resp["status"] == "error"
                assert "rate limit" in resp["message"].lower()
            finally:
                sock.close()
        finally:
            server.stop()
            thread.join(timeout=5)
