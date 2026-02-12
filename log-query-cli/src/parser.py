"""Log line parser — frozen dataclass + compiled regex."""

import re
from dataclasses import dataclass
from datetime import datetime

LOG_PATTERN = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]\s+\[(\w+)\]\s+(.*)"
)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class LogEntry:
    timestamp: datetime
    level: str
    message: str
    raw: str
    source_file: str


def parse_line(line: str, source_file: str = "") -> LogEntry | None:
    """Parse a single log line into a LogEntry. Returns None for unparseable lines."""
    stripped = line.rstrip("\n")
    match = LOG_PATTERN.match(stripped)
    if not match:
        return None

    timestamp_str, level, message = match.groups()
    try:
        timestamp = datetime.strptime(timestamp_str, TIMESTAMP_FORMAT)
    except ValueError:
        return None

    return LogEntry(
        timestamp=timestamp,
        level=level.upper(),
        message=message,
        raw=stripped,
        source_file=source_file,
    )
