"""Simple TCP log server for testing the log shipping client."""

import json
import logging
import socket
import threading

logger = logging.getLogger(__name__)


class SimpleLogServer:
    """TCP server that receives NDJSON log messages and sends acks.

    Stores received messages in self.received for test assertions.
    """

    def __init__(self, host: str, port: int, shutdown_event: threading.Event):
        self._host = host
        self._port = port
        self._shutdown = shutdown_event
        self._sock: socket.socket | None = None
        self._server_address: tuple | None = None
        self.received: list[dict] = []
        self._lock = threading.Lock()

    @property
    def server_address(self) -> tuple:
        return self._server_address

    def start(self):
        """Bind, listen, and accept connections until shutdown."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.settimeout(1.0)
        self._sock.bind((self._host, self._port))
        self._sock.listen(5)
        self._server_address = self._sock.getsockname()
        logger.info("Server listening on %s:%d", *self._server_address)

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
        self._shutdown.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def _handle_client(self, conn: socket.socket, addr: tuple):
        """Handle a single client connection: read NDJSON lines, send acks."""
        logger.info("Client connected from %s:%d", *addr)
        buf = b""
        conn.settimeout(1.0)

        try:
            while not self._shutdown.is_set():
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not data:
                    break

                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    self._process_line(conn, line)
        finally:
            conn.close()
            logger.info("Client disconnected: %s:%d", *addr)

    def _process_line(self, conn: socket.socket, line: bytes):
        """Parse one NDJSON line, validate, store, and ack."""
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            self._send_error(conn, "invalid JSON")
            return

        if "level" not in msg or "message" not in msg:
            self._send_error(conn, "missing required fields: level, message")
            return

        with self._lock:
            self.received.append(msg)
        logger.info("[%s] %s", msg["level"], msg["message"])

        ack = json.dumps({"status": "ok", "message": "received"}) + "\n"
        try:
            conn.sendall(ack.encode())
        except OSError:
            pass

    def _send_error(self, conn: socket.socket, reason: str):
        """Send an error response."""
        resp = json.dumps({"status": "error", "message": reason}) + "\n"
        try:
            conn.sendall(resp.encode())
        except OSError:
            pass
