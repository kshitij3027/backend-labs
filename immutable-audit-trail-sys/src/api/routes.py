"""HTTP route definitions."""
from __future__ import annotations

import time
from typing import Optional

import sqlalchemy as sa
from fastapi import APIRouter, Header, HTTPException, Query, Request, status

from src.api.models import AppendRequest, RecordResponse, RecordsList
from src.persistence.models import AuditRecord as AuditRecordORM
from src.settings import get_settings

router = APIRouter()


# --- Helpers ---------------------------------------------------------------

def _orm_to_response(row: AuditRecordORM) -> RecordResponse:
    return RecordResponse(
        seq=row.seq,
        timestamp_utc=row.timestamp_utc,
        actor=row.actor,
        action=row.action,
        resource=row.resource,
        success=row.success,
        error_message=row.error_message,
        processing_ms=row.processing_ms,
        args_digest=row.args_digest,
        result_digest=row.result_digest,
        prev_hash=row.prev_hash,
        self_hash=row.self_hash,
        signature=row.signature,
    )


def _resolve_actor(
    body_actor: Optional[str],
    header_user_id: Optional[str],
) -> str:
    """Body actor wins, then header X-User-ID, else anonymous fallback."""
    if body_actor:
        return body_actor
    if header_user_id:
        return header_user_id
    return get_settings().anonymous_user_id


# --- Health ---------------------------------------------------------------

@router.get("/api/health", tags=["health"])
async def health() -> dict[str, int | str]:
    return {"status": "healthy", "timestamp": int(time.time())}


# --- Audit append --------------------------------------------------------

@router.post(
    "/v1/audit/append",
    response_model=RecordResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["audit"],
)
async def audit_append(
    body: AppendRequest,
    request: Request,
    x_user_id: Optional[str] = Header(default=None, alias="X-User-ID"),
    x_session_id: Optional[str] = Header(default=None, alias="X-Session-ID"),
) -> RecordResponse:
    """Append a sealed audit record. Returns the new record (with seq, hashes, signature)."""
    appender = request.app.state.appender
    actor = _resolve_actor(body.actor, x_user_id)
    # x_session_id is captured for symmetry with the spec but not yet
    # stored — the schema doesn't have a session field. The header can
    # be added to args_digest by callers if they want it in the chain.
    record = await appender.append(
        actor=actor,
        action=body.action,
        resource=body.resource,
        success=body.success,
        args_digest=body.args_digest,
        result_digest=body.result_digest,
        processing_ms=body.processing_ms,
        error_message=body.error_message,
    )
    return RecordResponse(**record.model_dump())


# --- Audit records query -------------------------------------------------

@router.get(
    "/v1/records",
    response_model=RecordsList,
    tags=["audit"],
)
async def list_records(
    request: Request,
    actor: Optional[str] = Query(default=None),
    action: Optional[str] = Query(default=None),
    resource: Optional[str] = Query(default=None),
    from_ts: Optional[str] = Query(default=None, description="ISO-8601 UTC, inclusive"),
    to_ts: Optional[str] = Query(default=None, description="ISO-8601 UTC, inclusive"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> RecordsList:
    """Paginated, filterable records list — newest seq first."""
    factory = request.app.state.session_factory
    stmt = sa.select(AuditRecordORM)
    if actor is not None:
        stmt = stmt.where(AuditRecordORM.actor == actor)
    if action is not None:
        stmt = stmt.where(AuditRecordORM.action == action)
    if resource is not None:
        stmt = stmt.where(AuditRecordORM.resource == resource)
    if from_ts is not None:
        stmt = stmt.where(AuditRecordORM.timestamp_utc >= from_ts)
    if to_ts is not None:
        stmt = stmt.where(AuditRecordORM.timestamp_utc <= to_ts)
    stmt = stmt.order_by(AuditRecordORM.seq.desc()).offset(offset).limit(limit)

    async with factory() as session:
        rows = (await session.execute(stmt)).scalars().all()
    items = [_orm_to_response(r) for r in rows]
    return RecordsList(records=items, count=len(items), limit=limit, offset=offset)


@router.get(
    "/v1/records/{seq}",
    response_model=RecordResponse,
    tags=["audit"],
)
async def get_record(request: Request, seq: int) -> RecordResponse:
    factory = request.app.state.session_factory
    async with factory() as session:
        row = (
            await session.execute(
                sa.select(AuditRecordORM).where(AuditRecordORM.seq == seq)
            )
        ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail=f"record seq={seq} not found")
    return _orm_to_response(row)
