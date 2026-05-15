"""Protected log query endpoints. RBAC-gated, audit-trailed via middleware."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.auth.dependencies import CurrentUser, get_rbac
from src.data.mock_logs import KNOWN_RESOURCES, LogRecord, aggregate, mask_pii, search
from src.rbac.engine import RBACEngine
from src.schemas.logs import LogRecordOut, LogSearchResponse

router = APIRouter(prefix="/api/logs", tags=["logs"])


def _to_out(r: LogRecord) -> LogRecordOut:
    return LogRecordOut(
        id=r.id,
        resource=r.resource,
        timestamp=r.timestamp,
        level=r.level,
        message=r.message,
        fields=r.fields,
    )


@router.get("/search", response_model=LogSearchResponse)
async def search_logs(
    request: Request,
    user: CurrentUser,
    rbac: Annotated[RBACEngine, Depends(get_rbac)],
    resource: str = Query(..., description="Resource leaf, e.g. application.auth"),
    limit: int = Query(50, ge=1, le=500),
) -> LogSearchResponse:
    if resource not in KNOWN_RESOURCES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown resource {resource!r}; known: {sorted(KNOWN_RESOURCES)}",
        )

    requested = f"logs:read:{resource}"
    decision = rbac.check(user, requested)
    request.state.decision = decision  # picked up by AuditMiddleware

    if not decision.allow:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "forbidden",
                "rule": decision.rule,
                "reason": decision.reason,
            },
        )

    records = search(resource, limit=limit)

    if "aggregated_only" in decision.tags:
        return LogSearchResponse(
            resource=resource,
            count=len(records),
            aggregated=aggregate(records),
            masked=False,
            rbac_rule=decision.rule,
        )

    if "mask_pii" in decision.tags:
        records = [mask_pii(r) for r in records]
        return LogSearchResponse(
            resource=resource,
            count=len(records),
            records=[_to_out(r) for r in records],
            masked=True,
            rbac_rule=decision.rule,
        )

    return LogSearchResponse(
        resource=resource,
        count=len(records),
        records=[_to_out(r) for r in records],
        masked=False,
        rbac_rule=decision.rule,
    )
