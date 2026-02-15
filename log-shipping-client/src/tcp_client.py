"""TCP socket wrapper with send, receive, and reconnect logic."""

import json
import logging
import random
import socket
import threading

logger = logging.getLogger(__name__)


class TCPClient:
    """Manages a TCP connection to the log server with reconnect support."""

    def __init__(self, host: str, port: int, shutdown_event: threading.Event):
        self._host = host
        self._port = port
        self._shutdown = shutdown_event
        self._sock: socket.socket | None = None
        self._buffer = b""

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def connect(self) -> bool:
        """Establish a TCP connection. Returns True on success."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self._host, self._port))
            self._sock = sock
            self._buffer = b""
            logger.info("Connected to %s:%d", self._host, self._port)
            return True
        except OSError as e:
            logger.warning("Failed to connect to %s:%d: %s", self._host, self._port, e)
            return False

    def close(self):
        """Close the connection."""
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
            self._buffer = b""

    def send(self, data: bytes) -> bool:
        """Send data over the connection. Returns True on success."""
        if not self._sock:
            return False
        try:
            self._sock.sendall(data)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logger.warning("Send failed: %s", e)
            self.close()
            return False

    def recv_line(self) -> dict | None:
        """Read one newline-delimited JSON response. Returns parsed dict or None."""
        if not self._sock:
            return None

        while b"\n" not in self._buffer:
            try:
                chunk = self._sock.recv(4096)
            except (ConnectionResetError, OSError) as e:
                logger.warning("Recv failed: %s", e)
                self.close()
                return None
            if not chunk:
                logger.warning("Server closed connection")
                self.close()
                return None
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b"\n", 1)
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON response: %s", line[:200])
            return None

    def send_and_recv(self, data: bytes) -> dict | None:
        """Send data and read back one NDJSON response."""
        if not self.send(data):
            return None
        return self.recv_line()

    def connect_with_backoff(self, max_attempts: int = 0) -> bool:
        """Retry connection with exponential backoff and jitter.

        Args:
            max_attempts: Max number of attempts (0 = unlimited until shutdown).

        Returns:
            True if connected, False if exhausted attempts or shutdown requested.
        """
        base_delay = 1.0
        max_delay = 60.0
        attempt = 0

        while not self._shutdown.is_set():
            attempt += 1
            if self.connect():
                return True

            if max_attempts > 0 and attempt >= max_attempts:
                logger.error("Exhausted %d connection attempts", max_attempts)
                return False

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.3)
            total_delay = delay + jitter
            logger.info("Retrying in %.1fs (attempt %d)...", total_delay, attempt)
            self._shutdown.wait(total_delay)

        return False
