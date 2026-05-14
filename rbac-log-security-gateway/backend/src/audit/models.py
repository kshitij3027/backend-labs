"""Audit / security event data models. Frozen dataclasses for immutability."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional


Decision = Literal["allow", "deny", "n/a"]


@dataclass(frozen=True)
class AuditEntry:
    """One record per HTTP request — captured by AuditMiddleware."""
    timestamp: datetime
    user_id: Optional[str]
    username: Optional[str]
    method: str
    path: str
    status: int
    duration_ms: float
    source_ip: Optional[str]
    user_agent: Optional[str]
    decision: Decision = "n/a"
    rule: Optional[str] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class SecurityEvent:
    """A subset of AuditEntry that represents a 401/403 or auth failure. Tagged for separate query."""
    timestamp: datetime
    event_type: str  # e.g. "auth_failure", "authz_denied"
    username: Optional[str]
    path: str
    status: int
    source_ip: Optional[str]
    reason: Optional[str] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
