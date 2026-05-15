"""Pydantic response schemas for /api/admin endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel


class AuditSummaryResponse(BaseModel):
    total_entries: int
    by_status: Dict[int, int]
    by_user: Dict[str, int]
    allow_decisions: int
    deny_decisions: int
    security_events: int


class AuditEntryOut(BaseModel):
    timestamp: datetime
    username: Optional[str]
    method: str
    path: str
    status: int
    duration_ms: float
    source_ip: Optional[str]
    decision: str
    rule: Optional[str]
    reason: Optional[str]


class SecurityEventOut(BaseModel):
    timestamp: datetime
    event_type: str
    username: Optional[str]
    path: str
    status: int
    source_ip: Optional[str]
    reason: Optional[str]


class RBACPoliciesResponse(BaseModel):
    roles: Dict[str, List[str]]  # role -> permission strings
    default_scopes: Dict[str, str]


class SystemStatusResponse(BaseModel):
    status: str
    uptime_seconds: float
    audit_entry_count: int
    security_event_count: int
    known_roles: List[str]
    known_resources: List[str]
