"""Log entry formatter — builds structured JSON log entries."""

import socket
from datetime import datetime, timezone


def format_log_entry(seq: int, level: str, message: str, app: str = "udp-client",
                     host: str | None = None) -> dict:
    """Build a structured log entry dict."""
    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sequence": seq,
        "app": app,
        "level": level.upper(),
        "message": message,
        "host": host or socket.gethostname(),
    }
