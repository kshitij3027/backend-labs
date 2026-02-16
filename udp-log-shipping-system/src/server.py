"""UDP Log Server — receives JSON log messages over UDP."""

import json
import logging
import socket
import threading

from src.buffer import BufferedWriter
from src.config import Config
from src.error_tracker import ErrorTracker
from src.metrics import Metrics

logger = logging.getLogger(__name__)


class UDPLogServer:
    def __init__(self, config: Config, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._sock = None
        self._received_count = 0
        self._lock = threading.Lock()
        self.server_address = None
        self.metrics = Metrics()
        self.error_tracker = ErrorTracker(config.max_errors)
        self._writer = BufferedWriter(
            config.log_dir, config.log_filename,
            config.flush_count, config.flush_timeout_sec,
        )

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self._sock.settimeout(1.0)
        self._sock.bind((self._config.host, self._config.port))

        self.server_address = self._sock.getsockname()
        actual_rcvbuf = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        logger.info(
            "UDP server listening on %s:%d (SO_RCVBUF=%d bytes)",
            self.server_address[0], self.server_address[1], actual_rcvbuf,
        )

        while not self._shutdown.is_set():
            try:
                data, addr = self._sock.recvfrom(self._config.buffer_size)
            except socket.timeout:
                continue
            except OSError:
                if self._shutdown.is_set():
                    break
                raise

            try:
                message = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Invalid datagram from %s: %s", addr, exc)
                continue

            with self._lock:
                self._received_count += 1

            level = message.get("level", "UNKNOWN")
            self.metrics.increment(level)

            if level == "ERROR":
                self._writer.write_immediate(message)
                self.error_tracker.add(message)
            else:
                self._writer.append(message)

            logger.debug("Received from %s: %s", addr, message)

    def stop(self):
        self._shutdown.set()
        if self._sock:
            self._sock.close()
            self._sock = None
        self._writer.close()
        snap = self.metrics.snapshot()
        logger.info("UDP server stopped. Stats: %s", snap)

    @property
    def received_count(self) -> int:
        with self._lock:
            return self._received_count
