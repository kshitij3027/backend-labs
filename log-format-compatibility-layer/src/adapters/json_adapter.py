"""JSON log format adapter."""
import json
from datetime import datetime
from typing import Optional

from dateutil import parser as dateutil_parser

from src.adapters.base import LogFormatAdapter
from src.models import ParsedLog, SeverityLevel


# Key lookup orders for extracting fields from JSON log entries.
TIMESTAMP_KEYS = ("timestamp", "ts", "time", "@timestamp")
LEVEL_KEYS = ("level", "severity", "loglevel")
MESSAGE_KEYS = ("message", "msg")
HOSTNAME_KEYS = ("hostname", "host")
APP_NAME_KEYS = ("app_name", "application")
PID_KEYS = ("pid",)

# All keys that are consumed during extraction (not stored in metadata).
_CONSUMED_KEYS = set(TIMESTAMP_KEYS + LEVEL_KEYS + MESSAGE_KEYS + HOSTNAME_KEYS + APP_NAME_KEYS + PID_KEYS)


class JsonLogAdapter(LogFormatAdapter):
    """Adapter for parsing JSON-formatted log lines."""

    @property
    def format_name(self) -> str:
        return "json"

    def can_handle(self, line: str) -> float:
        """Return confidence score for whether *line* is a JSON log entry.

        Returns 0.95 for a valid JSON object (dict), 0.0 otherwise.
        """
        stripped = line.strip()
        if not stripped.startswith("{"):
            return 0.0
        try:
            data = json.loads(stripped)
            if isinstance(data, dict):
                return 0.95
        except (json.JSONDecodeError, ValueError):
            pass
        return 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract(data: dict, keys: tuple) -> Optional[str]:
        """Return the first matching value from *data* for the given *keys*."""
        for key in keys:
            if key in data:
                return data[key]
        return None

    @staticmethod
    def _parse_timestamp(raw_ts) -> Optional[datetime]:
        """Attempt to parse a timestamp value; return None on failure."""
        if raw_ts is None:
            return None
        try:
            return dateutil_parser.parse(str(raw_ts))
        except (ValueError, TypeError, OverflowError):
            return None

    @staticmethod
    def _parse_pid(raw_pid) -> Optional[int]:
        """Attempt to convert a PID value to int; return None on failure."""
        if raw_pid is None:
            return None
        try:
            return int(raw_pid)
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, line: str) -> ParsedLog:
        """Parse a JSON log line into a :class:`ParsedLog`.

        All keys that are not explicitly extracted into named fields are
        placed into the ``metadata`` dict.
        """
        try:
            data = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            return ParsedLog(raw=line, source_format=self.format_name, message=line)

        if not isinstance(data, dict):
            return ParsedLog(raw=line, source_format=self.format_name, message=line)

        # Extract known fields ------------------------------------------
        raw_ts = self._extract(data, TIMESTAMP_KEYS)
        timestamp = self._parse_timestamp(raw_ts)

        raw_level = self._extract(data, LEVEL_KEYS)
        level = SeverityLevel.from_string(str(raw_level)) if raw_level is not None else None

        message = self._extract(data, MESSAGE_KEYS) or ""
        hostname = self._extract(data, HOSTNAME_KEYS)
        app_name = self._extract(data, APP_NAME_KEYS)
        pid = self._parse_pid(self._extract(data, PID_KEYS))

        # Remaining keys → metadata ------------------------------------
        metadata = {k: v for k, v in data.items() if k not in _CONSUMED_KEYS}

        return ParsedLog(
            timestamp=timestamp,
            level=level,
            message=str(message),
            source_format=self.format_name,
            hostname=hostname,
            app_name=app_name,
            pid=pid,
            metadata=metadata,
            raw=line,
        )
