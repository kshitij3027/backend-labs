"""Per-client connection handler — NDJSON framing over TCP."""

import json
import logging
import socket

from src.config import Config
from src.filter import should_accept
from src.persistence import LogPersistence
from src.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


def _send_response(conn: socket.socket, status: str, message: str):
    """Send a JSON response line back to the client."""
    response = json.dumps({"status": status, "message": message}) + "\n"
    try:
        conn.sendall(response.encode("utf-8"))
    except OSError:
        pass


def handle_client(conn: socket.socket, addr: tuple, config: Config,
                  persistence: LogPersistence, rate_limiter: RateLimiter,
                  shutdown_event):
    """Handle a single client connection. Runs in its own thread."""
    client_ip = addr[0]
    logger.info("Client connected: %s:%d", addr[0], addr[1])
    conn.settimeout(1.0)

    buffer = b""
    try:
        while not shutdown_event.is_set():
            try:
                data = conn.recv(config.buffer_size)
            except socket.timeout:
                continue
            except OSError:
                break

            if not data:
                break

            buffer += data

            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue

                _process_line(conn, line, client_ip, config, persistence,
                              rate_limiter)
    finally:
        conn.close()
        logger.info("Client disconnected: %s:%d", addr[0], addr[1])


def _process_line(conn: socket.socket, line: bytes, client_ip: str,
                  config: Config, persistence: LogPersistence,
                  rate_limiter: RateLimiter):
    """Process a single NDJSON line from the client."""
    # Rate limit check
    if not rate_limiter.allow(client_ip):
        _send_response(conn, "error", "rate limit exceeded")
        return

    # Parse JSON
    try:
        msg = json.loads(line.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        _send_response(conn, "error", "invalid JSON")
        return

    # Validate required fields
    if not isinstance(msg, dict):
        _send_response(conn, "error", "expected JSON object")
        return
    if "level" not in msg or "message" not in msg:
        _send_response(conn, "error", "missing required fields: level, message")
        return

    level = str(msg["level"])
    message = str(msg["message"])

    # Filter by log level
    if not should_accept(level, config.min_log_level):
        _send_response(conn, "ok", "filtered")
        return

    # Persist
    persistence.write(level, message)

    _send_response(conn, "ok", "received")
