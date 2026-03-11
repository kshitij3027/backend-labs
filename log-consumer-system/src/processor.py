"""Log line parser and message processor."""

from __future__ import annotations

import re
from datetime import datetime

import structlog

from src.models import LogEntry

logger = structlog.get_logger(__name__)

# Apache/Nginx combined log format regex
# Example: 192.168.1.1 - - [10/Mar/2026:13:55:36 +0000] "GET /api/users HTTP/1.1" 200 1234 "-" "curl/7.68" 45.2
_LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+)\s+'           # client IP
    r'\S+\s+'                    # ident (usually -)
    r'\S+\s+'                    # auth user (usually -)
    r'\[(?P<timestamp>[^\]]+)\]\s+'  # [timestamp]
    r'"(?P<method>\S+)\s+'       # "METHOD
    r'(?P<path>\S+)\s+'          # /path
    r'\S+"\s+'                   # HTTP/x.x"
    r'(?P<status>\d{3})\s+'      # status code
    r'(?P<size>\d+)'             # response size
    r'(?:\s+"(?P<referrer>[^"]*)"\s+"(?P<useragent>[^"]*)")?'  # optional referrer & UA
    r'(?:\s+(?P<response_time>[\d.]+))?'  # optional response time in ms
    r'\s*$'
)

_TIMESTAMP_FMT = "%d/%b/%Y:%H:%M:%S %z"


class LogProcessor:
    """Parses raw log lines and processes Redis stream messages."""

    def parse_log_line(self, raw: str) -> LogEntry | None:
        """Parse an Apache/Nginx combined log format line into a LogEntry.

        Returns None if the line cannot be parsed.
        """
        if not raw or not raw.strip():
            return None

        match = _LOG_PATTERN.match(raw.strip())
        if not match:
            logger.debug("failed to parse log line", raw=raw[:100])
            return None

        groups = match.groupdict()

        # Parse timestamp
        ts = None
        try:
            ts = datetime.strptime(groups["timestamp"], _TIMESTAMP_FMT)
        except (ValueError, TypeError):
            logger.debug("failed to parse timestamp", timestamp=groups.get("timestamp"))

        # Parse optional response time
        response_time: float | None = None
        if groups.get("response_time"):
            try:
                response_time = float(groups["response_time"])
            except (ValueError, TypeError):
                pass

        try:
            return LogEntry(
                ip=groups["ip"],
                method=groups["method"],
                path=groups["path"],
                status_code=int(groups["status"]),
                response_size=int(groups["size"]),
                response_time_ms=response_time,
                timestamp=ts,
                raw=raw.strip(),
            )
        except Exception:
            logger.exception("failed to construct LogEntry from parsed groups")
            return None

    def process_message(self, msg_data: dict) -> LogEntry | None:
        """Extract and parse the 'log' field from a Redis stream message dict.

        Returns None if the key is missing or the line can't be parsed.
        """
        log_line = msg_data.get("log")
        if log_line is None:
            logger.debug("message dict missing 'log' key", msg_data=msg_data)
            return None
        return self.parse_log_line(log_line)
