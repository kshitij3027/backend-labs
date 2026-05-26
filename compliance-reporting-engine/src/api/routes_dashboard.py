"""HTMX dashboard routes: top-level shell + per-card HTMX partials.

The dashboard is purposely server-rendered: each card is a tiny Jinja
partial returned by its own ``GET /partials/<name>`` endpoint, polled
on a configurable interval. No JSON wire format, no client-side
templating, no Node toolchain — just HTML over the wire.

Commit 15 lands the shell + the stats card. Commit 16 adds the
recent / breakdown / in-flight partials below. Commit 17 layers in
the FinHealth card on top.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.stats_service import (
    compute_dashboard_stats,
    framework_breakdown,
    list_finhealth_reports,
    list_in_flight_reports,
    list_recent_reports,
)
from .dependencies import get_session


router = APIRouter(tags=["dashboard"])


@router.get("/", response_class=HTMLResponse)
async def dashboard_root(request: Request) -> HTMLResponse:
    """Render the dashboard shell.

    The shell ships a single stats ``<section>`` for now; each
    subsequent commit (16, 17) drops in additional ``hx-get`` cards
    that swap their innerHTML on the same polling interval.
    """
    templates = request.app.state.templates
    refresh_ms = request.app.state.settings.dashboard_refresh_ms
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"refresh_ms": refresh_ms},
    )


@router.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    """Render the stats card body (no chrome) for HTMX innerHTML swap.

    The aggregator does all the heavy lifting — we just shape the
    response into a Jinja partial.
    """
    stats = await compute_dashboard_stats(session)
    return request.app.state.templates.TemplateResponse(
        request,
        "_stats_card.html",
        {"stats": stats},
    )


@router.get("/partials/recent", response_class=HTMLResponse)
async def recent_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    """Render the recent-reports card body for HTMX innerHTML swap.

    Returns the last 10 ``Report`` rows ordered by ``created_at`` DESC.
    The shaping (short id, formatted timestamp, conditional download
    URL) happens in the service layer so the Jinja template stays
    declarative.
    """
    reports = await list_recent_reports(session, limit=10)
    return request.app.state.templates.TemplateResponse(
        request,
        "_recent_card.html",
        {"reports": reports},
    )


@router.get("/partials/breakdown", response_class=HTMLResponse)
async def breakdown_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    """Render the framework-breakdown card body for HTMX innerHTML swap.

    Each segment of the stacked bar is sized via inline ``width:%`` from
    the integer percentage the service computes — no client-side JS or
    SVG library needed. Returns an empty placeholder when no reports
    exist yet.
    """
    items = await framework_breakdown(session)
    return request.app.state.templates.TemplateResponse(
        request,
        "_breakdown_card.html",
        {"items": items},
    )


@router.get("/partials/inflight", response_class=HTMLResponse)
async def inflight_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    """Render the in-flight-reports card body for HTMX innerHTML swap.

    Surfaces reports currently in non-terminal states (PENDING,
    AGGREGATING, EXPORTING, SIGNING) so an operator can watch the
    pipeline drain on the same polling cadence as the rest of the
    dashboard.
    """
    reports = await list_in_flight_reports(session)
    return request.app.state.templates.TemplateResponse(
        request,
        "_inflight_card.html",
        {"reports": reports},
    )


@router.get("/partials/finhealth", response_class=HTMLResponse)
async def finhealth_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HTMLResponse:
    """Render the FinHealth bonus card body for HTMX innerHTML swap.

    Surfaces the last 5 FinHealth reports with dual-signature status
    pills (primary / secondary HMAC presence) so an auditor can see at
    a glance which composite-framework reports are fully attested.
    """
    reports = await list_finhealth_reports(session, limit=5)
    return request.app.state.templates.TemplateResponse(
        request,
        "_finhealth_card.html",
        {"reports": reports},
    )
