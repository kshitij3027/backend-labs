"""UDP log server — receives and processes batched log messages."""

import socket
import threading
import logging

from src.config import ServerConfig
from src.serializer import deserialize_batch

logger = logging.getLogger(__name__)


class UDPLogServer:
    def __init__(self, config: ServerConfig, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._sock = None
        self._received_count = 0
        self._batch_count = 0
        self._lock = threading.Lock()
        self.server_address = None

    def start(self):
        """Bind the UDP socket and enter the receive loop."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self._config.buffer_size
        )
        self._sock.settimeout(1.0)
        self._sock.bind((self._config.host, self._config.port))

        self.server_address = self._sock.getsockname()
        logger.info(
            "UDP server listening on %s:%d",
            self.server_address[0],
            self.server_address[1],
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
                entries = deserialize_batch(data)
            except Exception as exc:
                logger.warning("Invalid datagram from %s: %s", addr, exc)
                continue

            with self._lock:
                self._batch_count += 1
                self._received_count += len(entries)

            for entry in entries:
                logger.info("Processing log: %s", entry)

            print(f"Received batch of {len(entries)} logs from {addr}")

    def stop(self):
        """Signal shutdown and close the socket."""
        self._shutdown.set()
        if self._sock:
            self._sock.close()
            self._sock = None
        logger.info(
            "UDP server stopped. Received %d batches, %d total entries",
            self._batch_count,
            self._received_count,
        )

    @property
    def received_count(self) -> int:
        with self._lock:
            return self._received_count

    @property
    def batch_count(self) -> int:
        with self._lock:
            return self._batch_count
