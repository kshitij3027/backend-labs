"""Log format parsers for JSON and Apache combined log formats."""

import json
import re
from datetime import datetime

# Apache combined log format regex
APACHE_PATTERN = re.compile(
    r"^(?P<ip>\S+) \S+ \S+ "
    r"\[(?P<timestamp>[^\]]+)\] "
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r"(?P<status>\d+) (?P<size>\d+) "
    r'"(?P<referrer>[^"]*)" '
    r'"(?P<user_agent>[^"]*)"$'
)


def parse_json_line(line: str) -> dict | None:
    """Parse a JSON log line. Returns None on parse failure."""
    try:
        return json.loads(line.strip())
    except (json.JSONDecodeError, ValueError):
        return None


def parse_apache_line(line: str) -> dict | None:
    """Parse an Apache combined format log line.

    Normalizes to the same dict structure as JSON logs:
    - timestamp: ISO format string
    - level: derived from status code (2xx/3xx=INFO, 4xx=WARN, 5xx=ERROR)
    - service: "web-server"
    - message: "METHOD /path"
    - ip: source IP
    - url: request path
    - status_code: HTTP status as int
    - user_agent: UA string
    """
    line = line.strip()
    if not line:
        return None

    match = APACHE_PATTERN.match(line)
    if not match:
        return None

    d = match.groupdict()
    status = int(d["status"])

    # Derive level from status code
    if status < 400:
        level = "INFO"
    elif status < 500:
        level = "WARN"
    else:
        level = "ERROR"

    # Parse Apache timestamp to ISO format
    # Format: DD/Mon/YYYY:HH:MM:SS +0000
    try:
        ts = datetime.strptime(d["timestamp"], "%d/%b/%Y:%H:%M:%S %z")
        timestamp = ts.isoformat()
    except ValueError:
        timestamp = d["timestamp"]

    return {
        "timestamp": timestamp,
        "level": level,
        "service": "web-server",
        "message": f"{d['method']} {d['path']}",
        "ip": d["ip"],
        "url": d["path"],
        "status_code": status,
        "user_agent": d["user_agent"],
    }


def detect_format(file_path: str) -> str:
    """Detect log file format by reading the first non-empty line.

    Returns "json", "apache", or "unknown".
    """
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Try JSON first
            try:
                json.loads(line)
                return "json"
            except (json.JSONDecodeError, ValueError):
                pass
            # Try Apache
            if APACHE_PATTERN.match(line):
                return "apache"
            return "unknown"
    return "unknown"


def parse_line(line: str, fmt: str) -> dict | None:
    """Parse a log line given its format."""
    if fmt == "json":
        return parse_json_line(line)
    elif fmt == "apache":
        return parse_apache_line(line)
    return None
