"""Log line parsers: text and JSON formats with auto-detection."""

import json
import logging

from src.models import LogEntry

logger = logging.getLogger(__name__)

_parse_errors = 0


def get_parse_error_count() -> int:
    return _parse_errors


def parse_text_line(line: str, source_file: str) -> LogEntry | None:
    """Parse a pipe-delimited text log line.

    Expected format:
        2025-05-14 10:23:45 | INFO    | abc-1234 | user-service | user-67890 | req-xyz789 | 142ms | User login successful
    """
    global _parse_errors
    try:
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 8:
            _parse_errors += 1
            return None

        # Extract duration_ms from "142ms" format
        duration_raw = parts[6]
        duration_ms = int(duration_raw.replace("ms", ""))

        return LogEntry(
            timestamp=parts[0],
            level=parts[1],
            id=parts[2],
            service=parts[3],
            user_id=parts[4],
            request_id=parts[5],
            duration_ms=duration_ms,
            message="|".join(parts[7:]).strip(),  # rejoin in case message contains |
            source_file=source_file,
            raw=line,
        )
    except (ValueError, IndexError) as e:
        _parse_errors += 1
        logger.debug("Failed to parse text line: %s", e)
        return None


def parse_json_line(line: str, source_file: str) -> LogEntry | None:
    """Parse a JSON log line.

    Expected keys: timestamp, level, id, service, user_id, request_id, duration_ms, message
    """
    global _parse_errors
    try:
        data = json.loads(line)
        return LogEntry(
            timestamp=data["timestamp"],
            level=data["level"],
            id=data["id"],
            service=data["service"],
            user_id=data["user_id"],
            request_id=data["request_id"],
            duration_ms=int(data["duration_ms"]),
            message=data["message"],
            source_file=source_file,
            raw=line,
        )
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        _parse_errors += 1
        logger.debug("Failed to parse JSON line: %s", e)
        return None


def parse_line(line: str, source_file: str) -> LogEntry | None:
    """Auto-detect format and parse. Lines starting with '{' try JSON first."""
    stripped = line.strip()
    if not stripped:
        return None
    if stripped.startswith("{"):
        return parse_json_line(stripped, source_file)
    return parse_text_line(stripped, source_file)
