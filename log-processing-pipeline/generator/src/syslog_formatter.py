"""Produces RFC 3164 syslog lines."""

import random
from datetime import datetime, timezone

from generator.src.pools import SYSLOG_HOSTS, SYSLOG_TAGS, MESSAGE_POOL

_FACILITY = 1
_SEVERITY_MAP = {"DEBUG": 7, "INFO": 6, "WARNING": 4, "ERROR": 3}
_LEVEL_WEIGHTS = {"INFO": 60, "WARNING": 20, "ERROR": 10, "DEBUG": 10}


def generate_syslog_line() -> str:
    """Return a single RFC 3164 syslog line."""
    level = random.choices(
        list(_LEVEL_WEIGHTS.keys()),
        weights=list(_LEVEL_WEIGHTS.values()),
        k=1,
    )[0]
    severity = _SEVERITY_MAP[level]
    priority = _FACILITY * 8 + severity

    now = datetime.now(timezone.utc)
    time_str = now.strftime("%b %d %H:%M:%S")

    hostname = random.choice(SYSLOG_HOSTS)
    tag = random.choice(SYSLOG_TAGS)
    pid = random.randint(100, 99999)
    message = random.choice(MESSAGE_POOL)

    return f"<{priority}>{time_str} {hostname} {tag}[{pid}]: {message}"
