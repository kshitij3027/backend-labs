"""Normalized log entry dataclass — all formats map to this schema."""

from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class ParsedLogEntry:
    raw: str
    parsed: bool
    source_format: str  # "apache", "nginx", "json", "syslog", "unknown"

    timestamp: str | None = None
    remote_host: str | None = None
    method: str | None = None
    path: str | None = None
    protocol: str | None = None
    status_code: int | None = None
    body_bytes: int | None = None
    referer: str | None = None
    user_agent: str | None = None
    level: str | None = None
    message: str | None = None
    service: str | None = None
    extras: dict[str, Any] | None = None
    hostname: str | None = None
    priority: int | None = None
    facility: int | None = None
    severity: int | None = None
    tag: str | None = None
    pid: int | None = None


def entry_to_dict(entry: ParsedLogEntry) -> dict[str, Any]:
    """Convert a ParsedLogEntry to a dict, dropping None values for cleaner JSON."""
    return {k: v for k, v in asdict(entry).items() if v is not None}
