"""Log entry data model and ID generation helpers."""

import uuid
import random
from dataclasses import dataclass
from datetime import datetime


@dataclass
class LogEntry:
    timestamp: datetime
    level: str
    id: str
    service: str
    user_id: str
    request_id: str
    duration_ms: int
    message: str


def generate_short_id() -> str:
    """Produce IDs like 'abc-1234'."""
    u = uuid.uuid4().hex[:8]
    return f"{u[:3]}-{u[3:8]}"


def generate_user_id() -> str:
    return f"user-{random.randint(10000, 99999)}"


def generate_request_id() -> str:
    return f"req-{uuid.uuid4().hex[:6]}"


def generate_duration(level: str) -> int:
    """Return a realistic duration in ms, biased by log level."""
    ranges = {
        "DEBUG": (1, 50),
        "INFO": (10, 500),
        "WARNING": (50, 1000),
        "ERROR": (200, 5000),
    }
    lo, hi = ranges.get(level, (10, 500))
    return random.randint(lo, hi)
