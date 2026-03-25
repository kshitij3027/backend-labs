"""Multi-format timestamp parser producing UTC-aware datetimes."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")

# Apache combined log format: 24/Mar/2026:10:15:30 +0000
_APACHE_RE = re.compile(
    r"^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})\s([+-]\d{4})$"
)

# Syslog format: Mar 24 10:15:30
_SYSLOG_RE = re.compile(
    r"^([A-Za-z]{3})\s+(\d{1,2})\s(\d{2}):(\d{2}):(\d{2})$"
)


class TimestampParser:
    """Parse timestamps from multiple common formats into UTC datetimes."""

    def parse(self, raw: str | int | float) -> datetime:
        """Parse a raw timestamp value into a UTC-aware datetime.

        Supported formats:
        - ISO 8601 (with/without tz, with Z suffix, with milliseconds)
        - Unix epoch seconds (10-digit int/float)
        - Unix epoch milliseconds (13-digit int)
        - Apache combined log format
        - Syslog format (assumes current year, UTC)

        Raises:
            ValueError: If the input cannot be parsed.
        """
        if isinstance(raw, (int, float)):
            return self._parse_numeric(raw)

        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"Cannot parse empty or non-string timestamp: {raw!r}")

        raw = raw.strip()

        # Try Apache format first (very specific pattern)
        m = _APACHE_RE.match(raw)
        if m:
            return self._parse_apache(m)

        # Try syslog format
        m = _SYSLOG_RE.match(raw)
        if m:
            return self._parse_syslog(m)

        # Try ISO 8601
        return self._parse_iso(raw)

    def _parse_numeric(self, value: int | float) -> datetime:
        """Parse a numeric unix timestamp (seconds or milliseconds)."""
        if isinstance(value, int) and value > 9_999_999_999:
            # 13-digit integer -> milliseconds
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        return datetime.fromtimestamp(value, tz=UTC)

    def _parse_iso(self, raw: str) -> datetime:
        """Parse an ISO 8601 formatted string."""
        try:
            # Replace Z with +00:00 for fromisoformat compatibility
            normalized = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Cannot parse timestamp: {raw!r}") from exc

    def _parse_apache(self, m: re.Match) -> datetime:
        """Parse an Apache combined log format timestamp."""
        day, month_str, year, hour, minute, second, tz_offset = m.groups()
        # Build an ISO-like string for parsing
        iso_str = f"{year}-{_MONTH_MAP[month_str]}-{day}T{hour}:{minute}:{second}{tz_offset[:3]}:{tz_offset[3:]}"
        dt = datetime.fromisoformat(iso_str)
        return dt.astimezone(UTC)

    def _parse_syslog(self, m: re.Match) -> datetime:
        """Parse a syslog format timestamp (assumes current year, UTC)."""
        month_str, day, hour, minute, second = m.groups()
        year = datetime.now(tz=UTC).year
        month = _MONTH_MAP[month_str]
        iso_str = f"{year}-{month}-{int(day):02d}T{hour}:{minute}:{second}+00:00"
        return datetime.fromisoformat(iso_str)


_MONTH_MAP: dict[str, str] = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}
