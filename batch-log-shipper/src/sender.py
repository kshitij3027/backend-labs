"""UDP sender — sends data with retry and exponential backoff."""

import socket
import time
import random
import logging

logger = logging.getLogger(__name__)


class UDPSender:
    """Sends UDP datagrams to a target host with configurable retry logic."""

    def __init__(self, target_host: str, target_port: int, max_retries: int = 3):
        self._target_host = target_host
        self._target_port = target_port
        self._max_retries = max_retries
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data: bytes) -> bool:
        """Send data via UDP. Returns True on success, False after all retries exhausted."""
        for attempt in range(self._max_retries + 1):
            try:
                self._sock.sendto(data, (self._target_host, self._target_port))
                return True
            except OSError as exc:
                if attempt < self._max_retries:
                    logger.warning(
                        "Send failed (attempt %d/%d): %s",
                        attempt + 1,
                        self._max_retries + 1,
                        exc,
                    )
                    time.sleep(self._backoff_delay(attempt))
                else:
                    logger.error(
                        "Send failed after %d attempts: %s",
                        self._max_retries + 1,
                        exc,
                    )
        return False

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        """Calculate exponential backoff delay with jitter.

        Base delay doubles each attempt (0.1s, 0.2s, 0.4s, ...),
        capped at 2.0 seconds, then multiplied by a random jitter
        factor between 0.8 and 1.2.
        """
        base = 0.1 * (2 ** attempt)
        capped = min(base, 2.0)
        jitter = random.uniform(0.8, 1.2)
        return capped * jitter

    def close(self):
        """Close the underlying UDP socket."""
        self._sock.close()
