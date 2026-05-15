"""Admin-only endpoints: audit summary, security events, RBAC policies, system status.

All routes require the caller's user to have `logs:admin:*` (effectively admin role).
"""
from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.auth.dependencies import CurrentUser, get_rbac, get_audit
from src.audit.service import AuditService
from src.data.mock_logs import KNOWN_RESOURCES
from src.rbac.engine import RBACEngine
from src.rbac.roles import DEFAULT_SCOPES, ROLE_POLICIES
from src.schemas.admin import (
    AuditEntryOut,
    AuditSummaryResponse,
    RBACPoliciesResponse,
    SecurityEventOut,
    SystemStatusResponse,
)


router = APIRouter(prefix="/api/admin", tags=["admin"])

_APP_START_TIME = time.time()


def _require_admin(request: Request, user, rbac: RBACEngine) -> None:
    """Single-action admin check. Sets request.state.decision for the audit middleware."""
    decision = rbac.check(user, "logs:admin:*")
    request.state.decision = decision
    if not decision.allow:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "rule": decision.rule, "reason": decision.reason},
        )


@router.get("/audit-summary", response_model=AuditSummaryResponse)
async def audit_summary(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
    audit: Annotated[AuditService, Depends(get_audit)],
) -> AuditSummaryResponse:
    _require_admin(request, user, rbac)
    s = audit.summary()
    return AuditSummaryResponse(**s)


@router.get("/security-events", response_model=list[SecurityEventOut])
async def security_events(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
    audit: Annotated[AuditService, Depends(get_audit)],
    limit: int = Query(50, ge=1, le=500),
) -> list[SecurityEventOut]:
    _require_admin(request, user, rbac)
    return [
        SecurityEventOut(
            timestamp=e.timestamp,
            event_type=e.event_type,
            username=e.username,
            path=e.path,
            status=e.status,
            source_ip=e.source_ip,
            reason=e.reason,
        )
        for e in audit.security_events(limit=limit)
    ]


@router.get("/audit-entries", response_model=list[AuditEntryOut])
async def audit_entries(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
    audit: Annotated[AuditService, Depends(get_audit)],
    limit: int = Query(50, ge=1, le=500),
) -> list[AuditEntryOut]:
    """Recent audit entries (admin view). Newest first."""
    _require_admin(request, user, rbac)
    return [
        AuditEntryOut(
            timestamp=e.timestamp,
            username=e.username,
            method=e.method,
            path=e.path,
            status=e.status,
            duration_ms=e.duration_ms,
            source_ip=e.source_ip,
            decision=e.decision,
            rule=e.rule,
            reason=e.reason,
        )
        for e in audit.query(limit=limit)
    ]


@router.get("/rbac-policies", response_model=RBACPoliciesResponse)
async def rbac_policies(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
) -> RBACPoliciesResponse:
    _require_admin(request, user, rbac)
    return RBACPoliciesResponse(
        roles={role: [p.raw for p in perms] for role, perms in ROLE_POLICIES.items()},
        default_scopes=dict(DEFAULT_SCOPES),
    )


@router.get("/system-status", response_model=SystemStatusResponse)
async def system_status(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
    audit: Annotated[AuditService, Depends(get_audit)],
) -> SystemStatusResponse:
    _require_admin(request, user, rbac)
    summary = audit.summary()
    return SystemStatusResponse(
        status="ok",
        uptime_seconds=round(time.time() - _APP_START_TIME, 3),
        audit_entry_count=summary["total_entries"],
        security_event_count=summary["security_events"],
        known_roles=list(rbac.known_roles()),
        known_resources=list(KNOWN_RESOURCES),
    )
