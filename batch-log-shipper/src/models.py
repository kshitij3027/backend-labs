"""Log entry model."""

import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class LogEntry:
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    level: str = "INFO"
    message: str = ""
    service: str = "batch-log-shipper"
    metadata: dict = field(default_factory=dict)


def create_log_entry(
    level: str,
    message: str,
    service: str = "batch-log-shipper",
    metadata: Optional[dict] = None,
) -> LogEntry:
    """Factory function that creates a LogEntry."""
    return LogEntry(
        level=level,
        message=message,
        service=service,
        metadata=metadata if metadata is not None else {},
    )


def entry_to_dict(entry: LogEntry) -> dict:
    """Convert a LogEntry to a plain dictionary."""
    return asdict(entry)
