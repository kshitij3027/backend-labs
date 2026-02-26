"""Journald log format adapter.

Journald's default `short` output format mimics syslog RFC 3164 but
WITHOUT the <PRI> prefix.  Detection relies on structural features
(systemd references, unit suffixes, lifecycle verbs) scored by weight.
"""
import re
from datetime import datetime

from dateutil import parser as dateutil_parser

from src.adapters.base import LogFormatAdapter
from src.models import ParsedLog, SeverityLevel

# Journald short format (same as RFC 3164 minus the <PRI> header):
# timestamp hostname app_name[pid]: message
_JOURNALD_RE = re.compile(
    r'^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(\S+?)(?:\[(\d+)\])?:\s*(.*)'
)

# ---------- detection helpers ----------
_TIMESTAMP_RE = re.compile(r'^\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}')
_SYSTEMD_PID1_RE = re.compile(r'systemd\[1\]:')
_SYSTEMD_RE = re.compile(r'systemd')
_UNIT_SUFFIX_RE = re.compile(r'\.(service|socket|target|mount|timer|scope|slice)')
_LIFECYCLE_RE = re.compile(
    r'(Started|Stopped|Starting|Stopping|Reached target|Listening on|Mounted|Activated)'
)
_PROCESS_PID_RE = re.compile(r'\w+\[\d+\]:')
_KERNEL_RE = re.compile(r'kernel:')

# ---------- severity inference ----------
_ERROR_KEYWORDS = re.compile(
    r'\b(error|fail|failed|failure|critical|fatal|emergency|panic)\b', re.IGNORECASE
)
_WARNING_KEYWORDS = re.compile(
    r'\b(warning|warn|deprecated)\b', re.IGNORECASE
)
_DEBUG_KEYWORDS = re.compile(
    r'\b(debug|trace)\b', re.IGNORECASE
)


class JournaldAdapter(LogFormatAdapter):
    """Adapter for parsing journald short-format log lines."""

    @property
    def format_name(self) -> str:
        return "journald"

    def can_handle(self, line: str) -> float:
        """Return a weighted confidence score for *line* being journald output.

        Lines starting with ``<`` (syslog PRI) or ``{`` (JSON) are
        immediately rejected.  Otherwise, structural features are scored
        by weight and summed.
        """
        if line.startswith('<'):
            return 0.0
        if line.startswith('{'):
            return 0.0

        confidence = 0.0

        # Timestamp at start of line
        if _TIMESTAMP_RE.match(line):
            confidence += 0.3

        # systemd[1]: is a strong journald indicator
        if _SYSTEMD_PID1_RE.search(line):
            confidence += 0.4
        elif _SYSTEMD_RE.search(line):
            confidence += 0.2

        # Unit suffixes
        if _UNIT_SUFFIX_RE.search(line):
            confidence += 0.2

        # Lifecycle verbs
        if _LIFECYCLE_RE.search(line):
            confidence += 0.2

        # Process with PID pattern
        if _PROCESS_PID_RE.search(line):
            confidence += 0.1

        # Kernel marker
        if _KERNEL_RE.search(line):
            confidence += 0.1

        confidence = min(confidence, 1.0)
        return confidence if confidence > 0.0 else 0.0

    # ------------------------------------------------------------------ #
    #  Severity inference
    # ------------------------------------------------------------------ #

    @staticmethod
    def _infer_severity(message: str) -> SeverityLevel:
        """Infer the severity level from message content keywords."""
        if _ERROR_KEYWORDS.search(message):
            return SeverityLevel.ERROR
        if _WARNING_KEYWORDS.search(message):
            return SeverityLevel.WARNING
        if _DEBUG_KEYWORDS.search(message):
            return SeverityLevel.DEBUG
        return SeverityLevel.INFORMATIONAL

    # ------------------------------------------------------------------ #
    #  Parsing
    # ------------------------------------------------------------------ #

    def parse(self, line: str) -> ParsedLog:
        """Parse a journald short-format line into a ParsedLog."""
        match = _JOURNALD_RE.match(line)
        if not match:
            return ParsedLog(
                raw=line,
                source_format=self.format_name,
                message=line,
            )

        ts_str, hostname, app_name, pid_str, message = match.groups()

        # Parse timestamp — journald short timestamps lack year,
        # default to current year.
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

        level = self._infer_severity(message)

        return ParsedLog(
            timestamp=timestamp,
            level=level,
            message=message,
            source_format=self.format_name,
            hostname=hostname,
            app_name=app_name,
            pid=pid,
            raw=line,
        )
