"""Log line parsers: text and JSON formats with auto-detection."""

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
