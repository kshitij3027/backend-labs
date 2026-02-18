"""TCP client with framed send and exponential backoff reconnect."""

import logging
import random
import socket
import threading

from src.protocol import encode_frame, Algorithm

logger = logging.getLogger(__name__)


class TCPClient:
    def __init__(self, host: str, port: int, shutdown_event: threading.Event):
        self._host = host
        self._port = port
        self._shutdown = shutdown_event
        self._sock: socket.socket | None = None

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def connect(self) -> bool:
        """Establish TCP connection. Returns True on success."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self._host, self._port))
            self._sock = sock
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

    def send_frame(self, payload: bytes, compressed: bool, algorithm: Algorithm) -> bool:
        """Send a framed message. Returns True on success."""
        if not self._sock:
            return False
        try:
            frame = encode_frame(payload, compressed, algorithm)
            self._sock.sendall(frame)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logger.warning("Send failed: %s", e)
            self.close()
            return False

    def connect_with_backoff(self, max_attempts: int = 0) -> bool:
        """Retry connection with exponential backoff and jitter.
        max_attempts=0 means retry until shutdown."""
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
