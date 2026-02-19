"""Log entry creation helpers."""

import datetime


def create_log_entry(level: str, message: str, **extra) -> dict:
    """Create a log entry dict with ISO timestamp."""
    entry = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "level": level.upper(),
        "message": message,
    }
    entry.update(extra)
    return entry
