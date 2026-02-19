"""Integration tests — real TLS sockets, in-process server and client."""

import gzip
import json
import os
import socket
import ssl
import subprocess
import tempfile
import threading
import time

import pytest

from src.config import ServerConfig, ClientConfig
from src.server import TLSLogServer
from src.handler import handle_client, set_log_writer, set_metrics
from src.protocol import encode_frame, decode_frame_header, recv_exact
from src.tls_context import create_client_context_unverified, create_client_context_verified
from src.client import TLSLogClient
from src.models import create_log_entry


@pytest.fixture(scope="module")
def cert_dir():
    """Generate certs in a temp directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script = os.path.join(os.path.dirname(__file__), "..", "generate_certs.sh")
        result = subprocess.run(
            ["sh", script, tmpdir],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Cert gen failed: {result.stderr}"
        yield tmpdir


@pytest.fixture
def server_and_config(cert_dir, tmp_path):
    """Start a TLS server on a random port and return (server, config, shutdown_event)."""
    # Reset module-level state
    set_log_writer(None)
    set_metrics(None)

    config = ServerConfig(
        host="127.0.0.1",
        port=0,
        cert_file=os.path.join(cert_dir, "server.crt"),
        key_file=os.path.join(cert_dir, "server.key"),
        log_dir=str(tmp_path / "logs"),
        max_logs_per_file=10,
    )
    shutdown_event = threading.Event()
    server = TLSLogServer(config, shutdown_event)

    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()

    # Wait for server to bind
    for _ in range(50):
        if server.server_address is not None:
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("Server failed to bind")

    yield server, config, shutdown_event

    server.stop()
    thread.join(timeout=5)


def _raw_tls_connect(host, port):
    """Create a raw TLS connection (no cert verification)."""
    ctx = create_client_context_unverified()
    raw = socket.create_connection((host, port), timeout=5)
    return ctx.wrap_socket(raw, server_hostname=host)


def _send_log_raw(sock, log_entry):
    """Send a single log entry and receive the ack using raw sockets."""
    raw = json.dumps(log_entry).encode("utf-8")
    compressed = gzip.compress(raw)
    sock.sendall(encode_frame(compressed))

    ack_header = recv_exact(sock, 4)
    ack_len = decode_frame_header(ack_header)
    ack_data = recv_exact(sock, ack_len)
    return json.loads(ack_data.decode("utf-8"))


class TestBasicTransmission:
    """Test basic log transmission over TLS."""

    def test_single_log_send_and_ack(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address

        sock = _raw_tls_connect(host, port)
        try:
            entry = create_log_entry("INFO", "test message")
            ack = _send_log_raw(sock, entry)
            assert ack["status"] == "ok"
        finally:
            sock.close()

    def test_multiple_logs_same_connection(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address

        sock = _raw_tls_connect(host, port)
        try:
            for i in range(5):
                entry = create_log_entry("INFO", f"message {i}")
                ack = _send_log_raw(sock, entry)
                assert ack["status"] == "ok"
        finally:
            sock.close()

    def test_different_log_levels(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address

        sock = _raw_tls_connect(host, port)
        try:
            for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                entry = create_log_entry(level, f"{level} test")
                ack = _send_log_raw(sock, entry)
                assert ack["status"] == "ok"
        finally:
            sock.close()


class TestMultipleClients:
    """Test concurrent client connections."""

    def test_two_concurrent_clients(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address
        results = []

        def client_work(client_id):
            sock = _raw_tls_connect(host, port)
            try:
                for i in range(3):
                    entry = create_log_entry("INFO", f"client {client_id} msg {i}")
                    ack = _send_log_raw(sock, entry)
                    results.append(ack["status"])
            finally:
                sock.close()

        t1 = threading.Thread(target=client_work, args=(1,))
        t2 = threading.Thread(target=client_work, args=(2,))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert len(results) == 6
        assert all(s == "ok" for s in results)


class TestTLSClientClass:
    """Test the TLSLogClient class end-to-end."""

    def test_client_send_and_stats(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address

        client_config = ClientConfig(
            host=host,
            port=port,
            verify_certs=False,
        )
        client = TLSLogClient(client_config)
        try:
            client.connect()
            for i in range(3):
                client.send_log(create_log_entry("INFO", f"client test {i}"))
            assert client._logs_sent == 3
            assert client._total_raw > 0
            assert client._total_compressed > 0
        finally:
            client.close()


class TestCertVerification:
    """Test proper CA certificate verification."""

    def test_verified_client_connects(self, server_and_config, cert_dir):
        server, config, _ = server_and_config
        host, port = server.server_address

        ca_file = os.path.join(cert_dir, "ca.crt")
        client_config = ClientConfig(
            host="localhost",
            port=port,
            verify_certs=True,
            ca_file=ca_file,
        )
        client = TLSLogClient(client_config)
        try:
            client.connect()
            client.send_log(create_log_entry("INFO", "verified connection"))
            assert client._logs_sent == 1
        finally:
            client.close()

    def test_wrong_ca_fails(self, server_and_config):
        """Client with wrong CA should fail to connect."""
        server, config, _ = server_and_config
        host, port = server.server_address

        # Create a separate CA that doesn't match the server cert
        with tempfile.TemporaryDirectory() as wrong_dir:
            result = subprocess.run(
                ["sh", os.path.join(os.path.dirname(__file__), "..", "generate_certs.sh"), wrong_dir],
                capture_output=True, text=True,
            )
            assert result.returncode == 0

            client_config = ClientConfig(
                host="localhost",
                port=port,
                verify_certs=True,
                ca_file=os.path.join(wrong_dir, "ca.crt"),
            )
            client = TLSLogClient(client_config)
            with pytest.raises(ssl.SSLCertVerificationError):
                client.connect()
            client.close()


class TestRetryLogic:
    """Test client retry behavior."""

    def test_connect_with_retry_succeeds(self, server_and_config):
        server, config, _ = server_and_config
        host, port = server.server_address

        client_config = ClientConfig(
            host=host,
            port=port,
            verify_certs=False,
            retry_attempts=3,
            retry_base_delay=0.1,
        )
        client = TLSLogClient(client_config)
        try:
            client.connect_with_retry()
            client.send_log(create_log_entry("INFO", "retry test"))
            assert client._logs_sent == 1
        finally:
            client.close()

    def test_connect_with_retry_fails_on_bad_port(self):
        client_config = ClientConfig(
            host="127.0.0.1",
            port=1,  # unreachable port
            verify_certs=False,
            retry_attempts=2,
            retry_base_delay=0.1,
        )
        client = TLSLogClient(client_config)
        with pytest.raises((ConnectionRefusedError, OSError)):
            client.connect_with_retry()
        client.close()
