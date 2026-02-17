"""Tests for the UDP sender with retry logic."""

import socket

import pytest

from src.sender import UDPSender


@pytest.fixture
def udp_receiver():
    """Bind a UDP socket on an ephemeral port and yield (socket, port)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    _, port = sock.getsockname()
    yield sock, port
    sock.close()


class TestUDPSenderRealSocket:
    """Integration tests using a real loopback UDP socket."""

    def test_send_to_real_udp_socket(self, udp_receiver):
        receiver, port = udp_receiver
        sender = UDPSender("127.0.0.1", port)
        try:
            payload = b"hello from sender"
            sender.send(payload)
            receiver.settimeout(2.0)
            data, _addr = receiver.recvfrom(4096)
            assert data == payload
        finally:
            sender.close()

    def test_send_returns_true_on_success(self, udp_receiver):
        receiver, port = udp_receiver
        sender = UDPSender("127.0.0.1", port)
        try:
            result = sender.send(b"test data")
            assert result is True
        finally:
            sender.close()


class TestBackoffDelay:
    """Unit tests for the exponential backoff calculation."""

    def test_backoff_delay_calculation(self):
        """Verify delay grows exponentially: 0.1, 0.2, 0.4, 0.8 (before jitter)."""
        expected_bases = [0.1, 0.2, 0.4, 0.8]
        for attempt, expected_base in enumerate(expected_bases):
            delay = UDPSender._backoff_delay(attempt)
            # With jitter in [0.8, 1.2], delay should be in [base*0.8, base*1.2]
            assert expected_base * 0.8 <= delay <= expected_base * 1.2, (
                f"attempt={attempt}, delay={delay}, expected_base={expected_base}"
            )

    def test_max_delay_cap(self):
        """Large attempt numbers should be capped at 2.0 * jitter_max."""
        for _ in range(50):
            delay = UDPSender._backoff_delay(10)
            # Cap is 2.0, max jitter is 1.2 -> max possible delay is 2.4
            assert delay <= 2.0 * 1.2

    def test_jitter_range(self):
        """Calling _backoff_delay repeatedly for the same attempt should
        produce varying results (randomness via jitter)."""
        delays = {UDPSender._backoff_delay(2) for _ in range(100)}
        # With 100 samples of a continuous uniform distribution,
        # we should see many distinct values.
        assert len(delays) > 1, "Expected jitter to produce varying delays"


class TestClose:
    """Test that closing the sender prevents further sends."""

    def test_close(self):
        sender = UDPSender("127.0.0.1", 9999, max_retries=0)
        sender.close()
        # After closing, sendto should raise OSError
        result = sender.send(b"should fail")
        assert result is False
