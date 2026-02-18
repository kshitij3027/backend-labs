"""Log entry model with factory function."""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone


@dataclass
class LogEntry:
    timestamp: str
    level: str
    message: str
    service: str = "default"
    metadata: dict = field(default_factory=dict)


def create_log_entry(
    message: str,
    level: str = "INFO",
    service: str = "default",
    metadata: dict | None = None,
) -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        level=level,
        message=message,
        service=service,
        metadata=metadata or {},
    )


def entry_to_dict(entry: LogEntry) -> dict:
    return asdict(entry)
