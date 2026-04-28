"""Static Jinja-rendered dashboard for the Log Search API.

Mounted at the unauthenticated app root (``GET /``). The page itself is just
HTML/CSS/JS; all data flows through the ``/api/v1/...`` endpoints from the
browser, so authentication happens via the API calls (the dashboard is
a thin client of our own API).
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.config import Settings, get_settings

router = APIRouter(tags=["dashboard"])

# ``directory`` is resolved relative to the working directory at runtime, which
# is ``/app`` inside the container — the templates ship under ``src/templates``.
templates = Jinja2Templates(directory="src/templates")


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def render_dashboard(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> HTMLResponse:
    """Serve the single-page dashboard.

    Intentionally:
      * not rate-limited — it is a static resource on the app root,
      * not auth-protected — auth is performed by the JS via /api/v1/auth/token.
    """
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "project_name": settings.PROJECT_NAME,
            "api_v1_prefix": settings.API_V1_PREFIX,
        },
    )
