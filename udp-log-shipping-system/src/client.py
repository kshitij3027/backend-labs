"""UDP Log Client — sends JSON log messages over UDP."""

import json
import logging
import random
import socket
import time

from src.formatter import format_log_entry

logger = logging.getLogger(__name__)

LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
SAMPLE_MESSAGES = [
    "Application started successfully",
    "Processing user request",
    "Database query completed",
    "Cache miss for key: user_session",
    "Failed to connect to external API",
    "Disk usage above 90%",
    "Authentication token expired",
    "Request timeout after 30s",
    "New user registered",
    "Scheduled job completed",
]


class UDPLogClient:
    def __init__(self, server_host: str, server_port: int, app_name: str = "udp-client"):
        self._server_host = server_host
        self._server_port = server_port
        self._app_name = app_name
        self._seq = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("", 0))
        logger.info("Client bound to %s", self._sock.getsockname())

    def send_log(self, level: str, message: str):
        """Send a single log entry to the server."""
        self._seq += 1
        entry = format_log_entry(self._seq, level, message, self._app_name)
        data = json.dumps(entry).encode("utf-8")
        self._sock.sendto(data, (self._server_host, self._server_port))
        logger.debug("Sent seq=%d level=%s", self._seq, level)

    def generate_sample_logs(self, count: int, interval: float = 0.1):
        """Send N sample log messages with random levels."""
        for i in range(count):
            level = random.choice(LEVELS)
            message = random.choice(SAMPLE_MESSAGES)
            self.send_log(level, message)
            if interval > 0 and i < count - 1:
                time.sleep(interval)
        logger.info("Sent %d sample logs", count)

    def close(self):
        """Close the socket."""
        self._sock.close()
        logger.info("Client closed")
