"""Dashboard route + small helper for seeding sample data via the UI.

This module owns the user-facing HTML surface — the only HTML response
in the otherwise-JSON service. It also exposes a single helper
endpoint, ``POST /api/sample/seed``, so the dashboard's "Seed 500
sample logs" button can populate the index without the user having to
construct a 500-entry JSON body by hand.

The Jinja2 template directory is resolved relative to the project
root so the route works regardless of the process CWD inside the
container.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.models import LogAckResponse
from src.sample_data import generate_log_entries

# ``__file__`` is .../src/api/routes_dashboard.py; three ``parent`` hops
# land us at the project root where ``templates/`` lives.
ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES = Jinja2Templates(directory=str(ROOT / "templates"))

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Render the single-page dashboard.

    The template is intentionally minimal — Jinja2 only injects the
    page title; everything dynamic happens in ``static/app.js`` which
    talks back to the same FastAPI app over JSON.
    """
    return TEMPLATES.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"title": "Log Search & Reranker"},
    )


@router.post("/api/sample/seed", response_model=LogAckResponse)
async def seed_sample_logs(
    request: Request,
    count: int = Query(default=500, ge=1, le=10000),
) -> LogAckResponse:
    """Seed the index with deterministic synthetic log entries.

    Wraps :func:`src.sample_data.generate_log_entries` so a one-click
    button on the dashboard can populate the corpus without the user
    pasting a long JSON body. ``count`` is bounded to ``[1, 10000]``
    to mirror the bulk-ingest cap.
    """
    index = request.app.state.index
    entries = generate_log_entries(count, seed=0)
    doc_ids = await index.add_bulk(entries)
    return LogAckResponse(
        accepted=len(doc_ids),
        first_doc_id=doc_ids[0],
        last_doc_id=doc_ids[-1],
        index_version=index.version,
    )
