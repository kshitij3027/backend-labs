from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.api.dependencies import get_settings_dep
from src.settings import Settings

router = APIRouter(tags=["dashboard"])


def _templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates


@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> HTMLResponse:
    templates = _templates(request)
    return templates.TemplateResponse(
        request, "index.html", {"refresh_ms": settings.dashboard_refresh_sec * 1000}
    )


@router.get("/compare", response_class=HTMLResponse)
async def compare_view(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> HTMLResponse:
    templates = _templates(request)
    return templates.TemplateResponse(
        request, "compare.html", {"refresh_ms": settings.dashboard_refresh_sec * 1000}
    )
