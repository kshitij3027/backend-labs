"""Dashboard UI route.

Serves the single-page vanilla-JS dashboard at ``GET /``. The template
is rendered once per page load and hands control over to
``/static/app.js`` for all subsequent interactions (faceted search,
filter toggles, free-text highlight). Static assets are mounted at
``/static`` from ``src/main.py`` so this router only needs to ship
the HTML shell.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter(tags=["ui"])

# Relative to the container/host working directory (``/app`` in
# Docker, repo root locally). The FastAPI app is always launched with
# ``PYTHONPATH=/app``, so ``src/templates`` resolves correctly.
templates = Jinja2Templates(directory="src/templates")


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(request: Request) -> HTMLResponse:
    """Render the faceted-search dashboard shell."""
    return templates.TemplateResponse("index.html", {"request": request})
