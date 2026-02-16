"""Tests for the UDP log client."""

import json
import socket
import threading
import time

import pytest

from src.client import UDPLogClient


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
