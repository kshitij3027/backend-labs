"""HTMX dashboard routes: top-level shell + per-card HTMX partials.

The dashboard is purposely server-rendered: each card is a tiny Jinja
partial returned by its own ``GET /partials/<name>`` endpoint, polled
on a configurable interval. No JSON wire format, no client-side
templating, no Node toolchain — just HTML over the wire.

Commit 15 lands the shell + the stats card. Commits 16 and 17 add the
remaining partials (recent / breakdown / in-flight / finhealth).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..services.stats_service import compute_dashboard_stats
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
