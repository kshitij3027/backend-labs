"""FastAPI entrypoint for the real-time analytics dashboard.

Commit 1 scope: minimal skeleton with health check, Redis storage
wiring via lifespan, and static file serving.

* ``GET /`` — serves the dashboard HTML from ``static/index.html``.
* ``GET /health`` — liveness probe returning Redis connectivity status.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.config import get_config
from src.ingestion import extract_metrics, generate_sample_logs
from src.models import (
    GenerateResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
)
from src.storage import RedisStorage

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build singletons and manage the Redis connection lifecycle."""
    config = get_config()

    storage = RedisStorage(
        host=config.redis_host,
        port=config.redis_port,
        metric_ttl_seconds=config.metric_ttl_seconds,
    )
    await storage.connect()
    app.state.storage = storage

    logger.info(
        "real-time-analytics started (redis=%s:%d)",
        config.redis_host,
        config.redis_port,
    )

    try:
        yield
    finally:
        await storage.close()
        logger.info("real-time-analytics shut down")


app = FastAPI(title="Real-Time Analytics Dashboard", lifespan=lifespan)

# Mount static files if the directory exists.
if _STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Liveness/readiness probe reporting Redis connectivity."""
    storage: RedisStorage | None = getattr(app.state, "storage", None)
    connected = False
    if storage is not None:
        connected = await storage.ping()
    return HealthResponse(status="healthy", redis_connected=connected)


@app.post("/api/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest) -> IngestResponse:
    """Ingest raw log entries, extract metrics, and store them in Redis."""
    storage: RedisStorage = app.state.storage
    metrics = extract_metrics(request.logs)
    await storage.store_metrics_batch(metrics)
    return IngestResponse(
        ingested=len(request.logs),
        metrics_stored=len(metrics),
        services=list({m.service for m in metrics}),
    )


@app.post("/api/generate-sample-data", response_model=GenerateResponse)
async def generate_sample_data(
    service: str = "web-api",
    count: int = 50,
) -> GenerateResponse:
    """Generate sample log data, extract metrics, and store them in Redis."""
    storage: RedisStorage = app.state.storage
    logs = generate_sample_logs(service=service, count=count)
    metrics = extract_metrics(logs)
    await storage.store_metrics_batch(metrics)
    return GenerateResponse(
        logs_generated=len(logs),
        metrics_stored=len(metrics),
        services=[service],
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    """Serve the dashboard HTML.

    Returns a placeholder page if ``static/index.html`` doesn't exist yet
    (it will be created in a later commit).
    """
    index_path = _STATIC_DIR / "index.html"
    try:
        html = index_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        html = (
            "<!DOCTYPE html><html><head><title>Real-Time Analytics</title></head>"
            "<body><h1>Dashboard coming soon</h1></body></html>"
        )
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host=config.server_host,
        port=config.server_port,
        log_level="info",
    )
