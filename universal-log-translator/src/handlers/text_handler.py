"""Text/syslog log format handler with RFC 5424, RFC 3164, and generic timestamped support."""
import re
from datetime import datetime

from src.base_handler import BaseHandler
from src.models import LogEntry, LogLevel

# Syslog severity to LogLevel mapping
# 0=Emergency, 1=Alert, 2=Critical -> CRITICAL
# 3=Error -> ERROR
# 4=Warning -> WARNING
# 5=Notice, 6=Informational -> INFO
# 7=Debug -> DEBUG
SYSLOG_SEVERITY_MAP = {
    0: LogLevel.CRITICAL,
    1: LogLevel.CRITICAL,
    2: LogLevel.CRITICAL,
    3: LogLevel.ERROR,
    4: LogLevel.WARNING,
    5: LogLevel.INFO,
    6: LogLevel.INFO,
    7: LogLevel.DEBUG,
}

# RFC 5424: <priority>version timestamp hostname app-name procid msgid structured-data msg
RFC5424_PATTERN = re.compile(
    r"<(\d{1,3})>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S*)\s*(.*)"
)

# RFC 3164: <priority>Month Day HH:MM:SS hostname process[pid]: message
RFC3164_PATTERN = re.compile(
    r"<(\d{1,3})>(\w{3})\s+(\d{1,2})\s+(\d{2}:\d{2}:\d{2})\s+(\S+)\s+(\S+?)(?:\[(\d+)\])?:\s*(.*)"
)

# Generic timestamped: YYYY-MM-DD HH:MM:SS LEVEL message
GENERIC_TIMESTAMPED_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(\S+)\s+(.*)"
)


def _severity_from_priority(priority: int) -> LogLevel:
    """Extract severity from syslog priority and map to LogLevel.

    Priority = facility * 8 + severity, so severity = priority % 8.
    """
    severity = priority % 8
    return SYSLOG_SEVERITY_MAP.get(severity, LogLevel.UNKNOWN)


def _facility_from_priority(priority: int) -> int:
    """Extract facility from syslog priority.

    Priority = facility * 8 + severity, so facility = priority // 8.
    """
    return priority // 8


def _parse_syslog_timestamp(ts_str: str) -> datetime:
    """Parse a syslog timestamp string into a datetime."""
    # Try ISO 8601 first (RFC 5424 style)
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass

    # Try common syslog formats
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    return datetime.utcnow()


def _parse_rfc3164_timestamp(month: str, day: str, time_str: str) -> datetime:
    """Parse RFC 3164 timestamp components into a datetime.

    RFC 3164 timestamps lack a year, so we use the current year.
    """
    now = datetime.utcnow()
    ts_string = f"{now.year} {month} {day} {time_str}"
    try:
        return datetime.strptime(ts_string, "%Y %b %d %H:%M:%S")
    except ValueError:
        return now


def _parse_generic_timestamp(ts_str: str) -> datetime:
    """Parse a generic timestamp string."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass

    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    return datetime.utcnow()


class TextHandler(BaseHandler, format_name="text"):
    """Handler for text-based log formats: syslog (RFC 5424/3164) and generic timestamped text."""

    def can_handle(self, raw_data: bytes) -> bool:
        """Check if raw_data is valid UTF-8 text that looks like a log line.

        Returns True if the data matches RFC 5424, RFC 3164, generic timestamped text,
        or is valid plain text. Returns False for empty data, binary/non-text data,
        or JSON-like data (starts with { or [).
        """
        if not raw_data:
            return False

        # Reject data containing null bytes (binary indicator)
        if b"\x00" in raw_data:
            return False

        # Try to decode as UTF-8 (strict mode to reject binary data)
        try:
            text = raw_data.decode("utf-8").strip()
        except UnicodeDecodeError:
            return False

        if not text:
            return False

        # Reject data with excessive non-printable control characters (binary data)
        # Allow common whitespace: \t (0x09), \n (0x0A), \r (0x0D)
        control_count = sum(
            1 for c in text if ord(c) < 32 and c not in ("\t", "\n", "\r")
        )
        if control_count > 0:
            return False

        # Reject JSON-like data -- let the JSON handler deal with it
        first_char = text[0]
        if first_char in ("{", "["):
            return False

        # Accept if it matches any known text log pattern
        if RFC5424_PATTERN.match(text):
            return True
        if RFC3164_PATTERN.match(text):
            return True
        if GENERIC_TIMESTAMPED_PATTERN.match(text):
            return True

        # Fallback: accept any valid UTF-8 text
        return True

    def parse(self, raw_data: bytes) -> LogEntry:
        """Parse text bytes into a LogEntry.

        Tries patterns in order: RFC 5424 -> RFC 3164 -> generic timestamped -> fallback.

        Raises:
            ValueError: If the data cannot be decoded as UTF-8 text.
        """
        try:
            text = raw_data.decode("utf-8").strip()
        except UnicodeDecodeError as e:
            raise ValueError(f"Cannot decode as UTF-8 text: {e}") from e

        # Try RFC 5424
        match = RFC5424_PATTERN.match(text)
        if match:
            return self._parse_rfc5424(match, raw_data)

        # Try RFC 3164
        match = RFC3164_PATTERN.match(text)
        if match:
            return self._parse_rfc3164(match, raw_data)

        # Try generic timestamped
        match = GENERIC_TIMESTAMPED_PATTERN.match(text)
        if match:
            return self._parse_generic(match, raw_data)

        # Fallback: plain text
        return self._parse_fallback(text, raw_data)

    def _parse_rfc5424(self, match: re.Match, raw_data: bytes) -> LogEntry:
        """Parse an RFC 5424 syslog message."""
        priority = int(match.group(1))
        # version = match.group(2)  # not stored but used for detection
        timestamp_str = match.group(3)
        hostname = match.group(4)
        app_name = match.group(5)
        procid = match.group(6)
        msgid = match.group(7)
        structured_data = match.group(8)
        message = match.group(9)

        level = _severity_from_priority(priority)
        facility = _facility_from_priority(priority)
        timestamp = _parse_syslog_timestamp(timestamp_str)

        metadata = {
            "facility": facility,
            "priority": priority,
        }
        if procid and procid != "-":
            metadata["procid"] = procid
        if msgid and msgid != "-":
            metadata["msgid"] = msgid
        if structured_data and structured_data != "-":
            metadata["structured_data"] = structured_data

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            hostname=hostname,
            service=app_name if app_name != "-" else "",
            metadata=metadata,
            raw=raw_data,
            source_format="text",
        )

    def _parse_rfc3164(self, match: re.Match, raw_data: bytes) -> LogEntry:
        """Parse an RFC 3164 syslog message."""
        priority = int(match.group(1))
        month = match.group(2)
        day = match.group(3)
        time_str = match.group(4)
        hostname = match.group(5)
        process = match.group(6)
        pid = match.group(7)
        message = match.group(8)

        level = _severity_from_priority(priority)
        facility = _facility_from_priority(priority)
        timestamp = _parse_rfc3164_timestamp(month, day, time_str)

        metadata = {
            "facility": facility,
            "priority": priority,
        }
        if pid:
            metadata["pid"] = pid

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            hostname=hostname,
            service=process,
            metadata=metadata,
            raw=raw_data,
            source_format="text",
        )

    def _parse_generic(self, match: re.Match, raw_data: bytes) -> LogEntry:
        """Parse a generic timestamped log line."""
        timestamp_str = match.group(1)
        level_str = match.group(2)
        message = match.group(3)

        timestamp = _parse_generic_timestamp(timestamp_str)
        level = LogLevel.from_string(level_str)

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            raw=raw_data,
            source_format="text",
        )

    def _parse_fallback(self, text: str, raw_data: bytes) -> LogEntry:
        """Parse plain text as a fallback (no recognized pattern)."""
        return LogEntry(
            timestamp=datetime.utcnow(),
            level=LogLevel.UNKNOWN,
            message=text,
            raw=raw_data,
            source_format="text",
        )
