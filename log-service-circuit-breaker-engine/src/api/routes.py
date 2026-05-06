"""REST endpoints (GET only in this commit; POSTs land in Commit 11)."""
from __future__ import annotations
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from src.api.models import HealthResponse, MetricsResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    templates = request.app.state.templates
    if templates is None:
        return HTMLResponse("<html><body><h1>Circuit Breaker Engine</h1><p>Dashboard pending — Commit 14.</p></body></html>")
    # If templates dir is empty (Commit 14 hasn't landed yet), still render a fallback.
    try:
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception:
        return HTMLResponse("<html><body><h1>Circuit Breaker Engine</h1><p>Dashboard pending — Commit 14.</p></body></html>")


@router.get("/health", response_model=HealthResponse)
async def health(request: Request):
    uptime = time.time() - request.app.state.start_time
    return HealthResponse(status="ok", uptime_seconds=uptime)


@router.get("/api/metrics", response_model=MetricsResponse)
async def metrics(request: Request):
    registry = request.app.state.registry
    processor = request.app.state.processor
    return MetricsResponse(
        circuits=registry.metrics_snapshot(),
        processing=processor.get_processing_stats(),
        generated_at=time.time(),
    )


@router.get("/api/metrics/history")
async def metrics_history(request: Request):
    history = request.app.state.history
    return {"history": history.list()}
