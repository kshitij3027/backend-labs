"""Syslog RFC 5424 log format adapter."""
import re
from datetime import datetime
from typing import Optional

from dateutil import parser as dateutil_parser

from src.adapters.base import LogFormatAdapter
from src.config import FACILITY_MAP
from src.models import ParsedLog, SeverityLevel

# RFC 5424 syslog format:
# <priority>version timestamp hostname app_name procid msgid [structured_data] message
_RFC5424_RE = re.compile(
    r'^<(\d{1,3})>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(?:\[(.*?)\]\s*)?(.*)'
)

# Quick check pattern: priority followed by version number
_RFC5424_QUICK_RE = re.compile(r'^<\d{1,3}>\d+\s+')

# Pattern for parsing structured data blocks: [sdid key="value" ...]
_SD_BLOCK_RE = re.compile(r'\[(\S+?)\s+(.*?)\]')
_SD_KV_RE = re.compile(r'(\S+?)="(.*?)"')


def _parse_structured_data(sd_str: Optional[str]) -> dict:
    """Parse RFC 5424 structured data string into a dictionary.

    Args:
        sd_str: The structured data string content (without outer brackets),
                or None / "-" for nil values.

    Returns:
        Dictionary mapping SD-ID to its key-value pairs.
    """
    if not sd_str or sd_str.strip() == "-":
        return {}

    metadata = {}
    # Re-wrap so the block regex can find bracketed sections
    wrapped = f"[{sd_str}]"
    blocks = _SD_BLOCK_RE.findall(wrapped)
    for sd_id, params_str in blocks:
        kv_pairs = _SD_KV_RE.findall(params_str)
        metadata[sd_id] = {k: v for k, v in kv_pairs}
    return metadata


class SyslogRFC5424Adapter(LogFormatAdapter):
    """Adapter for parsing RFC 5424 syslog messages."""

    @property
    def format_name(self) -> str:
        return "syslog_rfc5424"

    def can_handle(self, line: str) -> float:
        """Return confidence score for whether *line* is an RFC 5424 syslog entry.

        Returns 0.95 if the line starts with <priority>version, 0.0 otherwise.
        """
        if _RFC5424_QUICK_RE.match(line):
            return 0.95
        return 0.0

    @staticmethod
    def _nil_to_none(value: str) -> Optional[str]:
        """Convert RFC 5424 nil value '-' to None."""
        if value == "-":
            return None
        return value

    def parse(self, line: str) -> ParsedLog:
        """Parse an RFC 5424 syslog line into a ParsedLog."""
        match = _RFC5424_RE.match(line)
        if not match:
            return ParsedLog(raw=line, source_format=self.format_name, message=line)

        (
            pri_str, version, ts_str, hostname, app_name,
            procid, msgid, sd_str, message
        ) = match.groups()

        # Decompose priority into facility and severity
        pri = int(pri_str)
        facility_code = pri >> 3
        severity_code = pri & 0x07

        facility_name = FACILITY_MAP.get(facility_code)
        level = SeverityLevel.from_syslog_severity(severity_code)

        # Parse timestamp
        timestamp = None
        ts_value = self._nil_to_none(ts_str)
        if ts_value:
            try:
                timestamp = dateutil_parser.parse(ts_value)
            except (ValueError, TypeError, OverflowError):
                pass

        # Handle nil values
        hostname = self._nil_to_none(hostname)
        app_name = self._nil_to_none(app_name)
        msgid = self._nil_to_none(msgid)

        # Parse PID from procid
        pid = None
        procid_value = self._nil_to_none(procid)
        if procid_value is not None:
            try:
                pid = int(procid_value)
            except (ValueError, TypeError):
                pass

        # Parse structured data
        metadata = _parse_structured_data(sd_str)
        if msgid:
            metadata["msgid"] = msgid

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
            metadata=metadata,
            raw=line,
        )
