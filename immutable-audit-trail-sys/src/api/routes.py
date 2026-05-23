"""HTTP route definitions."""
from __future__ import annotations

import time
from typing import Optional

import sqlalchemy as sa
from fastapi import APIRouter, Header, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse

from src.api.models import AppendRequest, RecordResponse, RecordsList
from src.chain.verifier import VerifyResult
from src.persistence.models import AuditRecord as AuditRecordORM
from src.reports.base import ReportBundle
from src.reports.gdpr import render_gdpr_report
from src.reports.hipaa import render_hipaa_report
from src.reports.soc2 import render_soc2_report
from src.reports.pci_dss import render_pci_dss_report
from src.settings import get_settings
from src.stats.counters import get_counters

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


# --- Chain integrity verification ---------------------------------------

@router.get(
    "/v1/verify",
    response_model=VerifyResult,
    tags=["audit"],
)
async def verify_chain(
    request: Request,
    from_seq: Optional[int] = Query(default=None, alias="from", ge=0),
    to_seq: Optional[int] = Query(default=None, alias="to", ge=0),
) -> VerifyResult:
    """Verify chain integrity.

    Both ``from`` and ``to`` optional. With neither: full chain. With both:
    inclusive range [from, to]. With one but not the other: 422 (we don't
    want to silently expand a half-specified range).
    """
    chain_verifier = request.app.state.chain_verifier
    if from_seq is None and to_seq is None:
        return await chain_verifier.verify_full()
    if from_seq is None or to_seq is None:
        raise HTTPException(
            status_code=422,
            detail="from and to must be specified together",
        )
    if to_seq < from_seq:
        raise HTTPException(
            status_code=422,
            detail=f"to ({to_seq}) must be >= from ({from_seq})",
        )
    return await chain_verifier.verify_range(from_seq, to_seq)


@router.get("/api/stats", tags=["observability"])
async def stats() -> dict[str, int]:
    """JSON snapshot of process-local counters. Prometheus exposition at /metrics."""
    return get_counters().snapshot()


# --- Compliance reports ---------------------------------------------------

from datetime import datetime, timedelta, timezone


@router.get(
    "/v1/reports/{framework}",
    response_model=ReportBundle,
    tags=["compliance"],
)
async def reports(
    request: Request,
    framework: str,
    from_ts: Optional[str] = Query(default=None, alias="from"),
    to_ts: Optional[str] = Query(default=None, alias="to"),
    actor: Optional[str] = Query(default=None),
    resource: Optional[str] = Query(default=None),
) -> ReportBundle:
    """Generate a compliance report for the given framework.

    Frameworks wired in C13: ``gdpr``, ``hipaa``. ``soc2``/``pci_dss`` land in C14.
    Time range defaults to ``[now - REPORT_DEFAULT_RANGE_DAYS, now]`` if unset.
    """
    settings = request.app.state.settings
    chain_verifier = request.app.state.chain_verifier
    signer = request.app.state.signer
    session_factory = request.app.state.session_factory

    if to_ts is None:
        to_ts = datetime.now(timezone.utc).isoformat()
    if from_ts is None:
        from_ts = (
            datetime.now(timezone.utc) - timedelta(days=settings.report_default_range_days)
        ).isoformat()

    fw = framework.lower()
    if fw == "gdpr":
        return await render_gdpr_report(
            session_factory=session_factory,
            chain_verifier=chain_verifier,
            signer=signer,
            from_ts=from_ts,
            to_ts=to_ts,
            actor=actor,
            resource=resource,
        )
    if fw == "hipaa":
        return await render_hipaa_report(
            session_factory=session_factory,
            chain_verifier=chain_verifier,
            signer=signer,
            from_ts=from_ts,
            to_ts=to_ts,
        )
    if fw == "soc2":
        return await render_soc2_report(
            session_factory=session_factory,
            chain_verifier=chain_verifier,
            signer=signer,
            from_ts=from_ts,
            to_ts=to_ts,
        )
    if fw == "pci_dss":
        return await render_pci_dss_report(
            session_factory=session_factory,
            chain_verifier=chain_verifier,
            signer=signer,
            from_ts=from_ts,
            to_ts=to_ts,
        )
    raise HTTPException(status_code=400, detail=f"unknown framework: {framework}")


# --- Dashboard ---------------------------------------------------------

# Cache the integrity check for 5s — verify_full scans the whole chain
# and the dashboard polls /partials/integrity every 10s by default.
_INTEGRITY_CACHE: dict = {"ts": 0.0, "result": None}
_INTEGRITY_TTL_SEC = 5.0


@router.get("/", response_class=HTMLResponse, tags=["dashboard"])
async def dashboard(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    settings = request.app.state.settings
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "refresh_ms": settings.dashboard_refresh_ms},
    )


@router.get("/partials/stats", response_class=HTMLResponse, tags=["dashboard"])
async def partial_stats(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "_stats_card.html",
        {"request": request, "stats": get_counters().snapshot()},
    )


@router.get("/partials/records", response_class=HTMLResponse, tags=["dashboard"])
async def partial_records(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    factory = request.app.state.session_factory
    async with factory() as session:
        rows = (await session.execute(
            sa.select(AuditRecordORM).order_by(AuditRecordORM.seq.desc()).limit(50)
        )).scalars().all()
    return templates.TemplateResponse(
        "_records_table.html",
        {"request": request, "records": rows},
    )


@router.get("/partials/integrity", response_class=HTMLResponse, tags=["dashboard"])
async def partial_integrity(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    now = time.time()
    if _INTEGRITY_CACHE["result"] is None or (now - _INTEGRITY_CACHE["ts"]) > _INTEGRITY_TTL_SEC:
        _INTEGRITY_CACHE["result"] = await request.app.state.chain_verifier.verify_full()
        _INTEGRITY_CACHE["ts"] = now
    return templates.TemplateResponse(
        "_integrity_card.html",
        {"request": request, "verify": _INTEGRITY_CACHE["result"]},
    )


@router.get("/partials/alerts", response_class=HTMLResponse, tags=["dashboard"])
async def partial_alerts(request: Request) -> HTMLResponse:
    templates = request.app.state.templates
    sink = request.app.state.alert_sink
    return templates.TemplateResponse(
        "_alerts_card.html",
        {"request": request, "alerts": sink.recent(50)},
    )
