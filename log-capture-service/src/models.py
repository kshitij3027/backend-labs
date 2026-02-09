"""Normalized log entry model for the capture pipeline."""

from dataclasses import dataclass, field


@dataclass
class LogEntry:
    timestamp: str       # ISO 8601
    level: str           # INFO, WARNING, ERROR, DEBUG
    id: str              # e.g. "abc-1234"
    service: str
    user_id: str
    request_id: str
    duration_ms: int
    message: str
    source_file: str     # which file this came from
    tags: list[str] = field(default_factory=list)
    raw: str = ""        # original unparsed line
