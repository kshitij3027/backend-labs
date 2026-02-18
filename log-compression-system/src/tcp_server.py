"""TCP log receiver — accepts connections, decodes frames, decompresses payloads."""

import json
import logging
import socket
import threading

from src.compression import CompressionHandler
from src.protocol import Algorithm, HEADER_SIZE, decode_frame_header, recv_exact

logger = logging.getLogger(__name__)


class TCPLogReceiver:
    def __init__(self, host: str, port: int, shutdown_event: threading.Event, metrics=None):
        self._host = host
        self._port = port
        self._shutdown = shutdown_event
        self._metrics = metrics
        self._sock = None
        self._server_address = None
        self._decompressor = CompressionHandler()  # Used for decompression only

    @property
    def server_address(self) -> tuple:
        """Return (host, port) the server is bound to."""
        return self._server_address

    def start(self):
        """Bind, listen, and accept connections until shutdown."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1.0)
        self._sock.bind((self._host, self._port))
        self._sock.listen(5)

        self._server_address = self._sock.getsockname()
        logger.info("Log receiver started on %s:%d", *self._server_address)

        while not self._shutdown.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            t = threading.Thread(
                target=self._handle_client,
                args=(conn, addr),
                daemon=True,
            )
            t.start()

    def stop(self):
        """Signal shutdown and close the listen socket."""
        logger.info("Server shutting down...")
        self._shutdown.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def _handle_client(self, conn: socket.socket, addr: tuple):
        """Handle a single client connection: read frames, decompress, parse."""
        logger.info("Client connected from %s:%d", *addr)
        try:
            while not self._shutdown.is_set():
                try:
                    header = recv_exact(conn, HEADER_SIZE)
                except ConnectionError:
                    break

                payload_length, is_compressed, algorithm = decode_frame_header(header)

                try:
                    payload = recv_exact(conn, payload_length)
                except ConnectionError:
                    break

                # Decompress if needed
                if is_compressed:
                    algo_name = algorithm.name.lower()  # "gzip" or "zlib"
                    data = self._decompressor.decompress(payload, algo_name)
                else:
                    data = payload

                # Parse JSON array of log entries
                try:
                    entries = json.loads(data.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning("Failed to parse payload: %s", e)
                    continue

                # Record metrics
                if self._metrics:
                    self._metrics.record_batch(
                        logs_count=len(entries),
                        compressed_size=payload_length,
                        decompressed_size=len(data),
                    )

                logger.info(
                    "Received batch: %d logs, %d bytes compressed → %d bytes",
                    len(entries), payload_length, len(data),
                )
        finally:
            conn.close()
            logger.info("Client disconnected: %s:%d", *addr)
