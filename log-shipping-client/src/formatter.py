"""Parse raw log lines into structured NDJSON messages."""

import json


def parse_log_line(line: str) -> dict | None:
    """Parse a log line in 'YYYY-MM-DD HH:MM:SS LEVEL Message' format.

    Returns a dict with timestamp, level, message keys, or None if unparseable.
    """
    stripped = line.strip()
    if not stripped:
        return None

    # Expected format: "2024-01-15 08:23:45 INFO Application started"
    # Date(10) + space(1) + Time(8) + space(1) + LEVEL + space + message
    parts = stripped.split(None, 3)
    if len(parts) < 4:
        return None

    date_str, time_str, level, message = parts
    timestamp = f"{date_str} {time_str}"

    # Basic validation: timestamp should have a dash, level should be alphabetic
    if "-" not in date_str or ":" not in time_str:
        return None
    if not level.isalpha():
        return None

    return {
        "timestamp": timestamp,
        "level": level.upper(),
        "message": message,
    }


def format_ndjson(entry: dict) -> bytes:
    """Serialize a dict to compact JSON + newline, encoded as UTF-8."""
    return (json.dumps(entry, separators=(",", ":")) + "\n").encode("utf-8")
