"""Regex-based parsers for Apache, Nginx, Syslog, and JSON log formats.

Auto-detect order:
  1. Starts with '{' → JSON
  2. Starts with '<' + digit → Syslog
  3. Try Nginx (more specific — has referer/user_agent)
  4. Try Apache (less specific)
  5. Unknown
"""

import json
import re
from datetime import datetime, timezone

from src.models import ParsedLogEntry

# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

_APACHE_RE = re.compile(
    r'^(?P<host>\S+) \S+ \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\d{3}|-) '
    r'(?P<size>\d+|-)$'
)

_NGINX_RE = re.compile(
    r'^(?P<host>\S+) - \S+ '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status>\d{3}|-) '
    r'(?P<size>\d+|-) '
    r'"(?P<referer>[^"]*)" '
    r'"(?P<user_agent>[^"]*)"$'
)

_SYSLOG_RE = re.compile(
    r'^<(?P<priority>\d{1,3})>'
    r'(?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}) '
    r'(?P<hostname>\S+) '
    r'(?P<tag>[\w/.\-]+)'
    r'(?:\[(?P<pid>\d+)\])?: '
    r'(?P<message>.*)$'
)

# Syslog severity → log level name
_SEVERITY_LEVELS = {
    0: "EMERGENCY", 1: "ALERT", 2: "CRITICAL", 3: "ERROR",
    4: "WARNING", 5: "NOTICE", 6: "INFO", 7: "DEBUG",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_request(request_str: str) -> tuple[str | None, str | None, str | None]:
    """Split 'GET /path HTTP/1.1' → (method, path, protocol)."""
    parts = request_str.split(" ", 2)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None, None, None


def _apache_time_to_iso(time_str: str) -> str:
    """Convert '10/Oct/2000:13:55:36 -0700' → ISO 8601."""
    try:
        dt = datetime.strptime(time_str, "%d/%b/%Y:%H:%M:%S %z")
        return dt.isoformat()
    except ValueError:
        return time_str


def _nginx_time_to_iso(time_str: str) -> str:
    """Nginx uses the same format as Apache."""
    return _apache_time_to_iso(time_str)


def _syslog_time_to_iso(time_str: str) -> str:
    """Convert 'Jan  5 14:30:01' → ISO 8601 (prepend current year)."""
    try:
        year = datetime.now(timezone.utc).year
        dt = datetime.strptime(f"{year} {time_str}", "%Y %b %d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return time_str


def _safe_int(value: str | None) -> int | None:
    """Convert string to int, returning None for '-' or invalid values."""
    if value is None or value == "-":
        return None
    try:
        return int(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Format-specific parsers
# ---------------------------------------------------------------------------


def _parse_apache(line: str) -> ParsedLogEntry | None:
    m = _APACHE_RE.match(line)
    if not m:
        return None
    method, path, protocol = _parse_request(m.group("request"))
    return ParsedLogEntry(
        raw=line,
        parsed=True,
        source_format="apache",
        timestamp=_apache_time_to_iso(m.group("time")),
        remote_host=m.group("host"),
        method=method,
        path=path,
        protocol=protocol,
        status_code=_safe_int(m.group("status")),
        body_bytes=_safe_int(m.group("size")),
    )


def _parse_nginx(line: str) -> ParsedLogEntry | None:
    m = _NGINX_RE.match(line)
    if not m:
        return None
    method, path, protocol = _parse_request(m.group("request"))
    referer = m.group("referer")
    return ParsedLogEntry(
        raw=line,
        parsed=True,
        source_format="nginx",
        timestamp=_nginx_time_to_iso(m.group("time")),
        remote_host=m.group("host"),
        method=method,
        path=path,
        protocol=protocol,
        status_code=_safe_int(m.group("status")),
        body_bytes=_safe_int(m.group("size")),
        referer=referer if referer != "-" else None,
        user_agent=m.group("user_agent"),
    )


def _parse_json(line: str) -> ParsedLogEntry | None:
    try:
        data = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None

    known_keys = {"timestamp", "level", "message", "service"}
    extras = {k: v for k, v in data.items() if k not in known_keys}

    return ParsedLogEntry(
        raw=line,
        parsed=True,
        source_format="json",
        timestamp=data.get("timestamp"),
        level=data.get("level"),
        message=data.get("message"),
        service=data.get("service"),
        extras=extras if extras else None,
    )


def _parse_syslog(line: str) -> ParsedLogEntry | None:
    m = _SYSLOG_RE.match(line)
    if not m:
        return None
    priority = int(m.group("priority"))
    facility = priority // 8
    severity = priority % 8

    return ParsedLogEntry(
        raw=line,
        parsed=True,
        source_format="syslog",
        timestamp=_syslog_time_to_iso(m.group("timestamp")),
        hostname=m.group("hostname"),
        tag=m.group("tag"),
        pid=_safe_int(m.group("pid")),
        message=m.group("message"),
        priority=priority,
        facility=facility,
        severity=severity,
        level=_SEVERITY_LEVELS.get(severity),
    )


# ---------------------------------------------------------------------------
# Auto-detect entry point
# ---------------------------------------------------------------------------


def parse_line(line: str) -> ParsedLogEntry:
    """Parse a single log line, auto-detecting the format.

    Returns a ParsedLogEntry with parsed=False if no format matched.
    """
    stripped = line.strip()
    if not stripped:
        return ParsedLogEntry(raw=line, parsed=False, source_format="unknown")

    # 1. JSON — starts with '{'
    if stripped.startswith("{"):
        result = _parse_json(stripped)
        if result:
            return result

    # 2. Syslog — starts with '<' followed by digit
    if stripped.startswith("<") and len(stripped) > 1 and stripped[1].isdigit():
        result = _parse_syslog(stripped)
        if result:
            return result

    # 3. Nginx (more specific — has referer + user_agent)
    result = _parse_nginx(stripped)
    if result:
        return result

    # 4. Apache (less specific)
    result = _parse_apache(stripped)
    if result:
        return result

    # 5. Unknown
    return ParsedLogEntry(raw=line, parsed=False, source_format="unknown")
