"""Dashboard routes — main page + HTMX-polled partials."""
from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
from src.persistence.models import ErasureAuditLog, ErasureRequest, RequestState
from src.services.stats_service import compute_statistics


router = APIRouter(tags=["dashboard"])

templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    settings = request.app.state.settings
    return templates.TemplateResponse(
        request, "dashboard.html",
        {"refresh_ms": settings.dashboard_refresh_ms},
    )


@router.get("/partials/stats", response_class=HTMLResponse)
async def partial_stats(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    stats = await compute_statistics(session)
    return templates.TemplateResponse(
        request, "_stats_card.html", {"stats": stats},
    )


_TERMINAL = (RequestState.COMPLETED, RequestState.FAILED)


@router.get("/partials/requests", response_class=HTMLResponse)
async def partial_requests(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    rows = (await session.execute(
        select(ErasureRequest)
        .where(ErasureRequest.state.notin_(_TERMINAL))
        .order_by(ErasureRequest.created_at.desc())
        .limit(10)
    )).scalars().all()
    view = [
        {
            "id": r.id,
            "user_id": r.user_id,
            "request_type": r.request_type.value,
            "state": r.state.value,
            "started_at": r.started_at,
        }
        for r in rows
    ]
    return templates.TemplateResponse(
        request, "_requests_card.html", {"rows": view},
    )


@router.get("/partials/completed", response_class=HTMLResponse)
async def partial_completed(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    rows = (await session.execute(
        select(ErasureRequest)
        .where(ErasureRequest.state.in_(_TERMINAL))
        .order_by(ErasureRequest.completed_at.desc().nullslast(), ErasureRequest.created_at.desc())
        .limit(10)
    )).scalars().all()

    def _dur(r: ErasureRequest) -> float | None:
        if r.started_at and r.completed_at:
            return (r.completed_at - r.started_at).total_seconds()
        return None

    view = [
        {
            "id": r.id,
            "user_id": r.user_id,
            "state": r.state.value,
            "completed_at": r.completed_at,
            "duration_s": _dur(r),
            "error_message": r.error_message,
        }
        for r in rows
    ]
    return templates.TemplateResponse(
        request, "_completed_card.html", {"rows": view},
    )


@router.get("/partials/audit", response_class=HTMLResponse)
async def partial_audit(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    rows = (await session.execute(
        select(ErasureAuditLog)
        .order_by(ErasureAuditLog.sequence.desc())
        .limit(20)
    )).scalars().all()
    view = [
        {
            "sequence": r.sequence,
            "event_type": r.event_type,
            "request_id": r.request_id,
            "created_at": r.created_at,
            "entry_hash": r.entry_hash,
        }
        for r in rows
    ]
    return templates.TemplateResponse(
        request, "_audit_card.html", {"rows": view},
    )
