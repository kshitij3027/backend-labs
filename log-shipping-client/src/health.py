"""Heartbeat health monitor for server availability."""

import logging
import socket
import threading

logger = logging.getLogger(__name__)


class HealthMonitor:
    """Periodically probes a TCP endpoint and tracks health state.

    Uses a simple TCP connect probe — if the connection succeeds,
    the server is healthy. Logs state transitions only.
    """

    def __init__(
        self,
        host: str,
        port: int,
        shutdown_event: threading.Event,
        interval: float = 10.0,
    ):
        self._host = host
        self._port = port
        self._shutdown = shutdown_event
        self._interval = interval
        self._healthy = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_healthy(self) -> bool:
        return self._healthy.is_set()

    def wait_for_healthy(self, timeout: float | None = None) -> bool:
        """Block until the server is healthy or timeout expires.

        Returns True if healthy, False if timed out.
        """
        return self._healthy.wait(timeout=timeout)

    def start(self):
        """Start the background health check thread."""
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Signal the monitor to stop."""
        self._shutdown.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _monitor_loop(self):
        """Periodically probe the server."""
        was_healthy = False
        while not self._shutdown.is_set():
            healthy = self._probe()

            if healthy and not was_healthy:
                logger.info("Server %s:%d is now healthy", self._host, self._port)
                self._healthy.set()
            elif not healthy and was_healthy:
                logger.warning("Server %s:%d is now unhealthy", self._host, self._port)
                self._healthy.clear()
            elif healthy and not was_healthy:
                # First check
                self._healthy.set()
            elif not healthy and not was_healthy:
                # Still unhealthy on first check
                self._healthy.clear()

            was_healthy = healthy
            self._shutdown.wait(self._interval)

    def _probe(self) -> bool:
        """Attempt a TCP connect to check server availability."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((self._host, self._port))
            sock.close()
            return True
        except OSError:
            return False
