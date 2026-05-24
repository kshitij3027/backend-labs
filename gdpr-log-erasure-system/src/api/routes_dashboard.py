"""Dashboard routes — main page + HTMX-polled partials."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.dependencies import get_session
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
