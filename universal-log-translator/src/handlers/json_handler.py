"""JSON log format handler with flexible key mapping."""
import json
from datetime import datetime

from src.base_handler import BaseHandler
from src.models import LogEntry, LogLevel

# Flexible key mappings: canonical key -> list of aliases
TIMESTAMP_KEYS = ["timestamp", "ts", "time", "@timestamp", "datetime", "date"]
LEVEL_KEYS = ["level", "severity", "log_level", "loglevel", "lvl"]
MESSAGE_KEYS = ["message", "msg", "text", "log", "body"]
SOURCE_KEYS = ["source"]
HOSTNAME_KEYS = ["hostname", "host"]
SERVICE_KEYS = ["service", "service_name", "app"]

# All known keys (used to determine what goes into metadata)
ALL_KNOWN_KEYS = set(
    TIMESTAMP_KEYS + LEVEL_KEYS + MESSAGE_KEYS + SOURCE_KEYS + HOSTNAME_KEYS + SERVICE_KEYS
)

# Common timestamp formats to try after ISO 8601
TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%d/%b/%Y:%H:%M:%S",
    "%b %d %H:%M:%S",
]


def _find_value(data: dict, keys: list[str]) -> str | None:
    """Find the first matching key in data, return its value or None."""
    for key in keys:
        if key in data:
            return str(data[key])
    return None


def _parse_timestamp(value: str | None) -> datetime:
    """Parse a timestamp string into a datetime, with fallback to now."""
    if not value:
        return datetime.utcnow()

    # Try datetime.fromisoformat first (handles most ISO 8601)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass

    # Try common formats
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Fallback to current time
    return datetime.utcnow()


class JsonHandler(BaseHandler, format_name="json"):
    """Handler for JSON-formatted log entries."""

    def can_handle(self, raw_data: bytes) -> bool:
        """Check if raw_data looks like JSON (starts with { or [)."""
        if not raw_data:
            return False
        try:
            text = raw_data.decode("utf-8", errors="replace").strip()
        except Exception:
            return False
        return len(text) > 0 and text[0] in ("{", "[")

    def parse(self, raw_data: bytes) -> LogEntry:
        """Parse JSON bytes into a LogEntry.

        Supports flexible key mapping for common log fields.
        Unknown keys are placed into metadata.
        JSON arrays are handled by parsing the first element.

        Raises:
            ValueError: If the data is not valid JSON.
        """
        try:
            text = raw_data.decode("utf-8", errors="replace").strip()
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e

        # Handle arrays by extracting the first element
        if isinstance(data, list):
            if not data:
                raise ValueError("Invalid JSON: empty array")
            data = data[0]

        if not isinstance(data, dict):
            raise ValueError("Invalid JSON: expected object or array of objects")

        # Extract known fields via flexible key mapping
        timestamp = _parse_timestamp(_find_value(data, TIMESTAMP_KEYS))
        level_str = _find_value(data, LEVEL_KEYS)
        level = LogLevel.from_string(level_str) if level_str else LogLevel.INFO
        message = _find_value(data, MESSAGE_KEYS) or ""
        source = _find_value(data, SOURCE_KEYS) or ""
        hostname = _find_value(data, HOSTNAME_KEYS) or ""
        service = _find_value(data, SERVICE_KEYS) or ""

        # Collect unrecognized keys into metadata
        metadata = {k: v for k, v in data.items() if k not in ALL_KNOWN_KEYS}

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            source=source,
            hostname=hostname,
            service=service,
            metadata=metadata,
            raw=raw_data,
            source_format="json",
        )
