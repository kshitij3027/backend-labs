"""Tests for the simple TCP log server."""

import json
import socket
import threading
import time

from src.server import SimpleLogServer


def _start_server():
    """Start a SimpleLogServer on a random port. Returns (server, host, port)."""
    shutdown = threading.Event()
    server = SimpleLogServer("127.0.0.1", 0, shutdown)
    t = threading.Thread(target=server.start, daemon=True)
    t.start()
    # Wait for server to bind
    for _ in range(50):
        if server.server_address:
            break
        time.sleep(0.02)
    host, port = server.server_address
    return server, host, port


def _send_and_recv(host, port, payload: bytes) -> dict:
    """Send payload and read one NDJSON response."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    sock.connect((host, port))
    sock.sendall(payload)
    buf = b""
    while b"\n" not in buf:
        buf += sock.recv(4096)
    sock.close()
    line = buf.split(b"\n")[0]
    return json.loads(line)


class TestSimpleLogServer:
    def test_valid_message(self):
        server, host, port = _start_server()
        try:
            msg = json.dumps({"level": "INFO", "message": "hello"}) + "\n"
            resp = _send_and_recv(host, port, msg.encode())
            assert resp["status"] == "ok"
            time.sleep(0.1)
            assert len(server.received) == 1
            assert server.received[0]["level"] == "INFO"
            assert server.received[0]["message"] == "hello"
        finally:
            server.stop()

    def test_invalid_json(self):
        server, host, port = _start_server()
        try:
            resp = _send_and_recv(host, port, b"not json\n")
            assert resp["status"] == "error"
            assert "invalid JSON" in resp["message"]
        finally:
            server.stop()

    def test_missing_fields(self):
        server, host, port = _start_server()
        try:
            msg = json.dumps({"level": "INFO"}) + "\n"
            resp = _send_and_recv(host, port, msg.encode())
            assert resp["status"] == "error"
            assert "missing" in resp["message"]
        finally:
            server.stop()

    def test_multiple_messages(self):
        server, host, port = _start_server()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((host, port))

            for i in range(3):
                msg = json.dumps({"level": "INFO", "message": f"msg {i}"}) + "\n"
                sock.sendall(msg.encode())
                buf = b""
                while b"\n" not in buf:
                    buf += sock.recv(4096)
                resp = json.loads(buf.split(b"\n")[0])
                assert resp["status"] == "ok"

            sock.close()
            time.sleep(0.1)
            assert len(server.received) == 3
        finally:
            server.stop()
