"""Syslog RFC 3164 log format adapter."""
import re
from datetime import datetime

from dateutil import parser as dateutil_parser

from src.adapters.base import LogFormatAdapter
from src.config import FACILITY_MAP
from src.models import ParsedLog, SeverityLevel

# RFC 3164 BSD syslog format:
# <priority>timestamp hostname app_name[pid]: message
_RFC3164_RE = re.compile(
    r'^<(\d{1,3})>(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(\S+?)(?:\[(\d+)\])?:\s*(.*)'
)


class SyslogRFC3164Adapter(LogFormatAdapter):
    """Adapter for parsing RFC 3164 (BSD) syslog messages."""

    @property
    def format_name(self) -> str:
        return "syslog_rfc3164"

    def can_handle(self, line: str) -> float:
        """Return confidence score for whether *line* is an RFC 3164 syslog entry.

        Returns 0.90 if the line matches the RFC 3164 pattern, 0.0 otherwise.
        """
        match = _RFC3164_RE.match(line)
        if match:
            return 0.90
        return 0.0

    def parse(self, line: str) -> ParsedLog:
        """Parse an RFC 3164 syslog line into a ParsedLog."""
        match = _RFC3164_RE.match(line)
        if not match:
            return ParsedLog(raw=line, source_format=self.format_name, message=line)

        pri_str, ts_str, hostname, app_name, pid_str, message = match.groups()

        # Decompose priority into facility and severity
        pri = int(pri_str)
        facility_code = pri >> 3
        severity_code = pri & 0x07

        facility_name = FACILITY_MAP.get(facility_code)
        level = SeverityLevel.from_syslog_severity(severity_code)

        # Parse timestamp — RFC 3164 timestamps lack year, default to current year
        timestamp = None
        try:
            timestamp = dateutil_parser.parse(
                ts_str, default=datetime(datetime.now().year, 1, 1)
            )
        except (ValueError, TypeError, OverflowError):
            pass

        # Parse PID
        pid = None
        if pid_str is not None:
            try:
                pid = int(pid_str)
            except (ValueError, TypeError):
                pass

        return ParsedLog(
            timestamp=timestamp,
            level=level,
            message=message,
            source_format=self.format_name,
            facility=facility_name,
            hostname=hostname,
            priority=pri,
            app_name=app_name,
            pid=pid,
            raw=line,
        )
