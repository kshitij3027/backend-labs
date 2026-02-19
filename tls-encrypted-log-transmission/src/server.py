"""TLS accept loop — spawns a thread per client connection."""

import logging
import socket
import ssl
import threading

from src.config import ServerConfig
from src.tls_context import create_server_context
from src.handler import handle_client

logger = logging.getLogger(__name__)


class TLSLogServer:
    """Multi-threaded TLS server for encrypted log collection."""

    def __init__(self, config: ServerConfig, shutdown_event: threading.Event):
        self._config = config
        self._shutdown_event = shutdown_event
        self._ssl_ctx = create_server_context(config.cert_file, config.key_file)
        self._sock = None
        self._server_address = None

    @property
    def server_address(self):
        return self._server_address

    @property
    def ssl_context(self):
        return self._ssl_ctx

    def start(self):
        """Bind, listen, and accept TLS connections until shutdown."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1.0)
        self._sock.bind((self._config.host, self._config.port))
        self._sock.listen(5)

        self._server_address = self._sock.getsockname()
        logger.info("TLS server listening on %s:%d", *self._server_address)
        print(f"[SERVER] SSL context loaded — cert: {self._config.cert_file}")
        print(f"[SERVER] Listening on {self._server_address[0]}:{self._server_address[1]}")

        while not self._shutdown_event.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            try:
                tls_conn = self._ssl_ctx.wrap_socket(conn, server_side=True)
            except ssl.SSLError as e:
                logger.warning("TLS handshake failed from %s: %s", addr, e)
                conn.close()
                continue

            logger.info("TLS connection from %s:%d", *addr)
            print(f"[SERVER] TLS connection from {addr[0]}:{addr[1]}")

            t = threading.Thread(
                target=handle_client,
                args=(tls_conn, addr, self._config, self._shutdown_event),
                daemon=True,
            )
            t.start()

    def stop(self):
        """Signal shutdown and close the listen socket."""
        logger.info("Server shutting down...")
        self._shutdown_event.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
