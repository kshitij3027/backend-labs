"""Parses RFC 3164 syslog lines into structured dicts."""

import re

_SYSLOG_RE = re.compile(
    r'^<(?P<priority>\d{1,3})>'
    r'(?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}) '
    r'(?P<hostname>\S+) '
    r'(?P<tag>[\w/.\-]+)'
    r'(?:\[(?P<pid>\d+)\])?: '
    r'(?P<message>.*)'
    r'$'
)

_SEVERITY_TO_LEVEL = {
    0: "ERROR",    # Emergency
    1: "ERROR",    # Alert
    2: "ERROR",    # Critical
    3: "ERROR",    # Error
    4: "WARNING",  # Warning
    5: "INFO",     # Notice
    6: "INFO",     # Informational
    7: "DEBUG",    # Debug
}


def parse_syslog_line(line: str) -> dict | None:
    m = _SYSLOG_RE.match(line)
    if not m:
        return None

    priority = int(m.group("priority"))
    facility = priority // 8
    severity = priority % 8
    level = _SEVERITY_TO_LEVEL.get(severity, "UNKNOWN")

    return {
        "timestamp": m.group("timestamp"),
        "hostname": m.group("hostname"),
        "tag": m.group("tag"),
        "pid": m.group("pid"),
        "message": m.group("message"),
        "level": level,
        "facility": facility,
        "format": "syslog",
        "raw": line,
    }
