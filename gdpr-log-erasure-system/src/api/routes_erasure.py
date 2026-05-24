"""Erasure request endpoints — POST submits + GET fetches status with audit trail."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.api.dependencies import get_session
from src.api.schemas import (
    AuditEntryResponse, ErasureRequestCreate, ErasureRequestResponse, ErasureRequestTypeIn,
)
from src.audit.chain import append_audit_entry
from src.persistence.models import (
    ErasureRequest, RequestState, RequestType,
)


router = APIRouter(prefix="/api", tags=["erasure"])


def _serialize(req: ErasureRequest) -> dict[str, Any]:
    """Convert an ORM ErasureRequest (with audit_entries eagerly loaded) to the response dict."""
    return {
        "id": req.id,
        "user_id": req.user_id,
        "request_type": req.request_type.value if hasattr(req.request_type, "value") else str(req.request_type),
        "state": req.state.value if hasattr(req.state, "value") else str(req.state),
        "error_message": req.error_message,
        "created_at": req.created_at,
        "started_at": req.started_at,
        "completed_at": req.completed_at,
        "audit_entries": [
            {
                "sequence": e.sequence,
                "event_type": e.event_type,
                "payload": e.payload_json,
                "prev_hash": e.prev_hash,
                "entry_hash": e.entry_hash,
                "created_at": e.created_at,
            }
            for e in (req.audit_entries or [])
        ],
    }


@router.post(
    "/erasure-requests",
    response_model=ErasureRequestResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_erasure_request(
    payload: ErasureRequestCreate,
    background_tasks: BackgroundTasks,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    """Create a new PENDING erasure request, audit the genesis-for-this-request,
    and schedule the coordinator to process it asynchronously.
    """
    req = ErasureRequest(
        user_id=payload.user_id,
        request_type=RequestType(payload.request_type.value),
        state=RequestState.PENDING,
    )
    session.add(req)
    await session.flush()  # populate req.id

    await append_audit_entry(
        session,
        request_id=req.id,
        event_type="REQUEST_CREATED",
        payload={
            "user_id": req.user_id,
            "request_type": req.request_type.value,
        },
    )
    await session.commit()

    # Reload with audit_entries eagerly joined for the response.
    result = (await session.execute(
        select(ErasureRequest)
        .options(selectinload(ErasureRequest.audit_entries))
        .where(ErasureRequest.id == req.id)
    )).scalar_one()

    # Schedule background processing; the coordinator lives on app.state.
    coordinator = request.app.state.coordinator
    background_tasks.add_task(coordinator.process, req.id)

    return _serialize(result)


@router.get(
    "/erasure-requests/{request_id}",
    response_model=ErasureRequestResponse,
)
async def get_erasure_request(
    request_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict[str, Any]:
    req = (await session.execute(
        select(ErasureRequest)
        .options(selectinload(ErasureRequest.audit_entries))
        .where(ErasureRequest.id == request_id)
    )).scalar_one_or_none()
    if req is None:
        raise HTTPException(status_code=404, detail=f"erasure request not found: {request_id}")
    return _serialize(req)
