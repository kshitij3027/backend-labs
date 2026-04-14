"""FastAPI entrypoint for the real-time analytics dashboard.

* ``GET /`` — serves the dashboard HTML from ``static/index.html``.
* ``GET /health`` — liveness probe returning Redis connectivity status.
* ``POST /api/ingest`` — ingest logs, extract metrics, detect anomalies.
* ``POST /api/generate-sample-data`` — generate and store sample data.
* ``GET /api/metrics/{service}/{metric_name}`` — query metrics with trend.
* ``GET /api/anomalies`` — query detected anomalies.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.config import get_config
from src.engine.anomalies import detect_anomalies
from src.engine.trends import calculate_trend
from src.ingestion import extract_metrics, generate_sample_logs
from src.models import (
    AnomalyResponse,
    GenerateResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    MetricResponse,
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
    """Ingest raw log entries, extract metrics, detect anomalies, and store."""
    storage: RedisStorage = app.state.storage
    config = get_config()
    metrics = extract_metrics(request.logs)
    await storage.store_metrics_batch(metrics)

    # Group metrics by (service, metric_name) and run anomaly detection
    anomalies_detected = 0
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for m in metrics:
        groups[(m.service, m.metric_name)].append(m)

    for (service, metric_name), group_points in groups.items():
        anomalies = detect_anomalies(group_points, threshold=config.anomaly_zscore_threshold)
        for anomaly in anomalies:
            await storage.store_anomaly(anomaly)
        anomalies_detected += len(anomalies)

    return IngestResponse(
        ingested=len(request.logs),
        metrics_stored=len(metrics),
        services=list({m.service for m in metrics}),
        anomalies_detected=anomalies_detected,
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


@app.get("/api/metrics/{service}/{metric_name}", response_model=MetricResponse)
async def get_metrics(
    service: str,
    metric_name: str,
    minutes: float = Query(default=60.0, gt=0, description="Time window in minutes"),
    include_trend: bool = Query(default=True, description="Include trend analysis"),
) -> MetricResponse:
    """Query stored metrics for a service/metric_name with optional trend analysis."""
    storage: RedisStorage = app.state.storage
    config = get_config()
    now = time.time()
    start_time = now - (minutes * 60)
    end_time = now

    data_points = await storage.get_metrics(service, metric_name, start_time, end_time)

    trend = None
    if include_trend and data_points:
        trend = calculate_trend(data_points, window_minutes=config.trend_window_minutes)

    return MetricResponse(
        service=service,
        metric_name=metric_name,
        data_points=data_points,
        count=len(data_points),
        trend=trend,
    )


@app.get("/api/anomalies", response_model=AnomalyResponse)
async def get_anomalies(
    hours: float = Query(default=1.0, gt=0, description="Lookback window in hours"),
    service: Optional[str] = Query(default=None, description="Filter by service name"),
    threshold: Optional[float] = Query(default=None, gt=0, description="Z-score threshold filter"),
) -> AnomalyResponse:
    """Query detected anomalies with optional service and threshold filters."""
    storage: RedisStorage = app.state.storage
    anomalies = await storage.get_anomalies(hours=hours)

    if service is not None:
        anomalies = [a for a in anomalies if a.service == service]

    if threshold is not None:
        anomalies = [a for a in anomalies if abs(a.z_score) >= threshold]

    return AnomalyResponse(
        anomalies=anomalies,
        count=len(anomalies),
        hours=hours,
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
