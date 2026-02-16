"""Tests for the UDP log client."""

import json
import socket
import threading
import time

import pytest

from src.client import UDPLogClient
from src.config import Config
from src.server import UDPLogServer


def _make_receiver(port=0):
    """Create a UDP socket to receive messages, returns (sock, address)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", port))
    sock.settimeout(5.0)
    return sock, sock.getsockname()


class TestClientSend:
    def test_sends_single_log(self):
        recv_sock, (host, port) = _make_receiver()
        try:
            client = UDPLogClient(host, port, app_name="test")
            client.send_log("INFO", "hello")

            data, addr = recv_sock.recvfrom(65536)
            entry = json.loads(data.decode("utf-8"))

            assert entry["level"] == "INFO"
            assert entry["message"] == "hello"
            assert entry["sequence"] == 1
            assert entry["app"] == "test"

            client.close()
        finally:
            recv_sock.close()

    def test_sequence_increments(self):
        recv_sock, (host, port) = _make_receiver()
        try:
            client = UDPLogClient(host, port)

            for i in range(5):
                client.send_log("INFO", f"msg-{i}")

            sequences = []
            for _ in range(5):
                data, _ = recv_sock.recvfrom(65536)
                entry = json.loads(data.decode("utf-8"))
                sequences.append(entry["sequence"])

            assert sequences == [1, 2, 3, 4, 5]
            client.close()
        finally:
            recv_sock.close()

    def test_generate_sample_logs(self):
        recv_sock, (host, port) = _make_receiver()
        try:
            client = UDPLogClient(host, port)
            client.generate_sample_logs(count=3, interval=0)

            entries = []
            for _ in range(3):
                data, _ = recv_sock.recvfrom(65536)
                entries.append(json.loads(data.decode("utf-8")))

            assert len(entries) == 3
            assert all("level" in e for e in entries)
            assert all("message" in e for e in entries)

            client.close()
        finally:
            recv_sock.close()


class TestClientAcks:
    def test_receives_ack_for_error_log(self, tmp_path):
        """Send an ERROR log to a real server and verify ACK is received."""
        config = Config(
            host="127.0.0.1", port=0,
            log_dir=str(tmp_path / "logs"), log_filename="test.log",
            flush_count=1000, flush_timeout_sec=60, max_errors=100,
        )
        shutdown = threading.Event()
        server = UDPLogServer(config, shutdown)
        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()

        for _ in range(50):
            if server.server_address is not None:
                break
            time.sleep(0.05)

        try:
            host, port = server.server_address
            client = UDPLogClient(host, port)
            client.send_log("ERROR", "disk full")
            time.sleep(0.5)

            acks = client.get_acks()
            assert len(acks) == 1
            assert 1 in acks
            assert acks[1]["ack"] is True
            assert acks[1]["sequence"] == 1

            client.close()
        finally:
            server.stop()
            thread.join(timeout=5)

    def test_no_ack_for_info_log(self, tmp_path):
        """INFO logs should not trigger ACKs."""
        config = Config(
            host="127.0.0.1", port=0,
            log_dir=str(tmp_path / "logs"), log_filename="test.log",
            flush_count=1000, flush_timeout_sec=60, max_errors=100,
        )
        shutdown = threading.Event()
        server = UDPLogServer(config, shutdown)
        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()

        for _ in range(50):
            if server.server_address is not None:
                break
            time.sleep(0.05)

        try:
            host, port = server.server_address
            client = UDPLogClient(host, port)
            client.send_log("INFO", "all good")
            time.sleep(0.5)

            acks = client.get_acks()
            assert len(acks) == 0

            client.close()
        finally:
            server.stop()
            thread.join(timeout=5)
