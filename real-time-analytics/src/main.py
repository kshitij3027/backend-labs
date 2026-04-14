"""FastAPI entrypoint for the real-time analytics dashboard.

* ``GET /`` — serves the dashboard HTML from ``static/index.html``.
* ``GET /health`` — liveness probe returning Redis connectivity status.
* ``POST /api/ingest`` — ingest logs, extract metrics, detect anomalies.
* ``POST /api/generate-sample-data`` — generate and store sample data.
* ``GET /api/metrics/{service}/{metric_name}`` — query metrics with trend.
* ``GET /api/anomalies`` — query detected anomalies.
* ``GET /api/services`` — list all known services.
* ``GET /api/export`` — export metrics as CSV or JSON.
* ``GET /api/ws-status`` — active WebSocket connection info.
* ``WS /ws`` — WebSocket endpoint for real-time streaming.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

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
from src.websocket import ConnectionManager, broadcast_loop, heartbeat_loop

logger = logging.getLogger(__name__)

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build singletons, start background tasks, manage the Redis lifecycle."""
    config = get_config()

    storage = RedisStorage(
        host=config.redis_host,
        port=config.redis_port,
        metric_ttl_seconds=config.metric_ttl_seconds,
    )
    await storage.connect()
    app.state.storage = storage

    # WebSocket infrastructure
    ws_manager = ConnectionManager()
    app.state.ws_manager = ws_manager

    stop_event = asyncio.Event()
    app.state.stop_event = stop_event

    # Start background tasks
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(ws_manager, config.ws_heartbeat_interval, stop_event),
    )
    broadcast_task = asyncio.create_task(
        broadcast_loop(ws_manager, storage, config, stop_event),
    )

    logger.info(
        "real-time-analytics started (redis=%s:%d)",
        config.redis_host,
        config.redis_port,
    )

    try:
        yield
    finally:
        # Signal background tasks to stop, then cancel them
        stop_event.set()
        heartbeat_task.cancel()
        broadcast_task.cancel()
        for task in (heartbeat_task, broadcast_task):
            try:
                await task
            except asyncio.CancelledError:
                pass
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
    all_anomalies = []
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for m in metrics:
        groups[(m.service, m.metric_name)].append(m)

    for (service, metric_name), group_points in groups.items():
        anomalies = detect_anomalies(group_points, threshold=config.anomaly_zscore_threshold)
        for anomaly in anomalies:
            await storage.store_anomaly(anomaly)
            all_anomalies.append(anomaly)
        anomalies_detected += len(anomalies)

    # Broadcast anomalies to WebSocket subscribers
    ws_manager: ConnectionManager = app.state.ws_manager
    if ws_manager.active_count > 0 and all_anomalies:
        for anomaly in all_anomalies:
            await ws_manager.broadcast_alert({
                "service": anomaly.service,
                "metric_name": anomaly.metric_name,
                "value": anomaly.value,
                "z_score": anomaly.z_score,
                "timestamp": anomaly.timestamp,
            })

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


@app.get("/api/services")
async def get_services() -> dict:
    """Return a list of all known service names."""
    storage: RedisStorage = app.state.storage
    services = await storage.get_services()
    return {"services": services}


@app.get("/api/export", response_model=None)
async def export_metrics(
    service: str = Query(..., description="Service name"),
    metric_name: str = Query(..., description="Metric name"),
    minutes: float = Query(default=60.0, gt=0, description="Time window in minutes"),
    format: str = Query(default="json", description="Export format: json or csv"),
):
    """Export metrics for a service/metric as CSV or JSON file download."""
    storage: RedisStorage = app.state.storage
    now = time.time()
    start_time = now - (minutes * 60)
    end_time = now

    data_points = await storage.get_metrics(service, metric_name, start_time, end_time)

    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["timestamp", "value", "service", "metric_name", "tags"])
        for p in data_points:
            writer.writerow([p.timestamp, p.value, p.service, p.metric_name, str(p.tags)])
        output.seek(0)

        filename = f"{service}_{metric_name}_{int(minutes)}m.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    else:
        # JSON export
        filename = f"{service}_{metric_name}_{int(minutes)}m.json"
        json_data = {
            "service": service,
            "metric_name": metric_name,
            "minutes": minutes,
            "data_points": [p.model_dump() for p in data_points],
            "count": len(data_points),
        }
        return JSONResponse(
            content=json_data,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


@app.get("/api/ws-status")
async def ws_status() -> dict:
    """Return active WebSocket connection count and per-stream subscription counts."""
    ws_manager: ConnectionManager = app.state.ws_manager
    return {
        "active_connections": ws_manager.active_count,
        "subscriptions": ws_manager.subscriptions_summary,
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint for real-time metric/alert streaming.

    Protocol:
    - Server sends ``{"type": "connected", ...}`` on connect.
    - Client sends ``{"type": "subscribe", "streams": [...]}`` to subscribe.
    - Client sends ``{"type": "unsubscribe", "streams": [...]}`` to unsubscribe.
    - Client sends ``{"type": "pong"}`` in reply to server pings.
    """
    manager: ConnectionManager = app.state.ws_manager
    client_id = await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type", "")

            if msg_type == "subscribe":
                streams = data.get("streams", [])
                subscribed = await manager.subscribe(client_id, streams)
                await websocket.send_json({
                    "type": "subscribed",
                    "streams": sorted(subscribed),
                })

            elif msg_type == "unsubscribe":
                streams = data.get("streams", [])
                await manager.unsubscribe(client_id, streams)
                await websocket.send_json({
                    "type": "unsubscribed",
                    "streams": streams,
                })

            elif msg_type == "pong":
                await manager.handle_pong(client_id)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await manager.disconnect(client_id)


@app.get("/")
async def dashboard():
    """Serve the dashboard HTML from ``static/index.html``.

    Returns a placeholder page if the file doesn't exist yet.
    """
    index_path = _STATIC_DIR / "index.html"
    if index_path.is_file():
        return FileResponse(str(index_path), media_type="text/html")
    return HTMLResponse(
        "<!DOCTYPE html><html><head><title>Real-Time Analytics</title></head>"
        "<body><h1>Dashboard coming soon</h1></body></html>"
    )


if __name__ == "__main__":
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host=config.server_host,
        port=config.server_port,
        log_level="info",
    )
