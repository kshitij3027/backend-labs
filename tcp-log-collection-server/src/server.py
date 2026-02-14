"""TCP accept loop — spawns a thread per client connection."""

import logging
import socket
import threading

from src.config import Config
from src.handler import handle_client
from src.persistence import LogPersistence
from src.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class TCPLogServer:
    """Multi-threaded TCP server for log collection."""

    def __init__(self, config: Config, shutdown_event: threading.Event):
        self._config = config
        self._shutdown_event = shutdown_event
        self._persistence = LogPersistence(
            config.log_dir,
            config.log_filename,
            config.enable_log_persistence,
        )
        self._rate_limiter = RateLimiter(
            config.rate_limit_enabled,
            config.rate_limit_max_requests,
            config.rate_limit_window_seconds,
        )
        self._sock = None
        self._server_address = None

    @property
    def server_address(self) -> tuple:
        """Return (host, port) the server is bound to. Useful when port=0."""
        return self._server_address

    def start(self):
        """Bind, listen, and accept connections until shutdown."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1.0)
        self._sock.bind((self._config.host, self._config.port))
        self._sock.listen(5)

        self._server_address = self._sock.getsockname()
        logger.info("Server listening on %s:%d", *self._server_address)

        while not self._shutdown_event.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, self._config, self._persistence,
                      self._rate_limiter, self._shutdown_event),
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
        self._persistence.close()
