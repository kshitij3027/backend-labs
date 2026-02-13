"""Parses Apache Combined Log Format lines into structured dicts.

Regex matches the sibling log-parsing-service/src/parsers.py _APACHE_RE.
"""

import re
from datetime import datetime

_APACHE_RE = re.compile(
    r'^(?P<host>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\d{3}|-) '
    r'(?P<size>\d+|-)$'
)


def _level_from_status(status: int) -> str:
    if status < 400:
        return "INFO"
    if status < 500:
        return "WARNING"
    return "ERROR"


def parse_apache_line(line: str) -> dict | None:
    """Parse a single Apache log line into a dict, or None if no match."""
    m = _APACHE_RE.match(line)
    if not m:
        return None

    status_str = m.group("status")
    status = int(status_str) if status_str != "-" else 0
    size_str = m.group("size")
    size = int(size_str) if size_str != "-" else 0

    request = m.group("request")
    parts = request.split(" ", 2)
    method = parts[0] if len(parts) >= 1 else ""
    path = parts[1] if len(parts) >= 2 else ""
    protocol = parts[2] if len(parts) >= 3 else ""

    # Parse Apache time format: 13/Feb/2026:06:47:53 +0000
    try:
        dt = datetime.strptime(m.group("time"), "%d/%b/%Y:%H:%M:%S %z")
        timestamp = dt.isoformat()
    except ValueError:
        timestamp = m.group("time")

    return {
        "timestamp": timestamp,
        "remote_host": m.group("host"),
        "method": method,
        "path": path,
        "protocol": protocol,
        "status_code": status,
        "body_bytes": size,
        "level": _level_from_status(status),
        "raw": line,
    }
