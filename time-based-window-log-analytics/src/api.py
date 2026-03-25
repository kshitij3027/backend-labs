"""FastAPI application for Time-Based Windowed Log Analytics."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
import structlog
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from src.aggregator import Aggregator
from src.config import AppConfig
from src.models import (
    BatchIngestRequest,
    BatchIngestResponse,
    IngestResponse,
    LogEvent,
    WindowInfo,
    WindowMetrics,
    WindowState,
)
from src.timestamp_parser import TimestampParser
from src.websocket import ConnectionManager
from src.window_manager import WindowManager
from src.window_rotator import WindowRotator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialise Redis, managers, and background scheduler."""
    config = AppConfig.from_env()

    # Logging
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )
    log = structlog.get_logger()
    log.info("starting", api_port=config.api_port, redis_host=config.redis_host)

    # Redis
    redis_url = f"redis://{config.redis_host}:{config.redis_port}"
    redis_client = aioredis.from_url(redis_url, decode_responses=False)

    # Components
    window_manager = WindowManager(redis_client, config)
    aggregator = Aggregator(redis_client)
    rotator = WindowRotator(redis_client, config)
    ts_parser = TimestampParser()

    # WebSocket manager
    ws_manager = ConnectionManager()

    # Store on app state
    app.state.config = config
    app.state.redis = redis_client
    app.state.window_manager = window_manager
    app.state.aggregator = aggregator
    app.state.rotator = rotator
    app.state.ts_parser = ts_parser
    app.state.start_time = int(time.time())
    app.state.ws_manager = ws_manager

    # Background scheduler for window rotation / cleanup
    loop = asyncio.get_event_loop()

    def _run_check() -> None:
        try:
            asyncio.run_coroutine_threadsafe(rotator.check_windows(), loop).result(timeout=30)
        except Exception:
            pass

    def _run_cleanup() -> None:
        try:
            asyncio.run_coroutine_threadsafe(rotator.cleanup_expired(), loop).result(timeout=30)
        except Exception:
            pass

    scheduler = BackgroundScheduler()
    scheduler.add_job(_run_check, "interval", seconds=config.lifecycle_check_interval)
    scheduler.add_job(_run_cleanup, "interval", seconds=config.cleanup_interval)
    scheduler.start()
    app.state.scheduler = scheduler

    # Broadcast loop — sends metrics to all WebSocket clients
    async def _broadcast_loop() -> None:
        while True:
            await asyncio.sleep(config.dashboard_refresh_interval)
            try:
                payload: dict = {}
                for wt in config.window_types:
                    keys = await aggregator.get_active_windows(wt.name)
                    windows_data: list[dict] = []
                    for wk in keys:
                        m = await aggregator.get_window_metrics(wk, wt.size_seconds)
                        if m:
                            windows_data.append(m.model_dump())
                    payload[wt.name] = {
                        "window_count": len(windows_data),
                        "windows": windows_data,
                    }
                if ws_manager.active_count > 0:
                    await ws_manager.broadcast({"type": "metrics_update", "data": payload})
            except Exception:
                pass

    broadcast_task = asyncio.create_task(_broadcast_loop())

    yield

    # Shutdown
    broadcast_task.cancel()
    scheduler.shutdown(wait=False)
    await redis_client.aclose()
    log.info("shutting_down")


app = FastAPI(
    title="Time-Based Windowed Log Analytics",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
async def _ingest_one(event: LogEvent) -> dict:
    """Process a single log event. Returns a result dict."""
    ts_parser: TimestampParser = app.state.ts_parser
    window_manager: WindowManager = app.state.window_manager
    aggregator: Aggregator = app.state.aggregator

    try:
        parsed_ts = ts_parser.parse(event.timestamp)
    except ValueError as exc:
        return {"accepted": 0, "rejected": 1, "late": 0, "error": str(exc)}

    assignments = await window_manager.assign_event(event, parsed_ts)

    accepted = 0
    late = 0
    for a in assignments:
        if a["accepted"]:
            await aggregator.record_event(a["window_key"], event, parsed_ts)
            accepted += 1
            if a.get("late"):
                late += 1

    return {"accepted": accepted, "rejected": 0, "late": late, "error": None}


@app.post("/api/v1/logs", response_model=IngestResponse)
async def ingest_single(event: LogEvent) -> IngestResponse:
    """Ingest a single log event."""
    result = await _ingest_one(event)
    errors = [result["error"]] if result["error"] else []
    return IngestResponse(
        accepted=result["accepted"],
        rejected=result["rejected"],
        late_accepted=result["late"],
        errors=errors,
    )


@app.post("/api/v1/logs/batch", response_model=BatchIngestResponse)
async def ingest_batch(body: BatchIngestRequest) -> BatchIngestResponse:
    """Ingest a batch of log events."""
    total_accepted = 0
    total_rejected = 0
    total_late = 0
    errors: list[str] = []

    for event in body.events:
        result = await _ingest_one(event)
        total_accepted += result["accepted"]
        total_rejected += result["rejected"]
        total_late += result["late"]
        if result["error"]:
            errors.append(result["error"])

    return BatchIngestResponse(
        total=len(body.events),
        accepted=total_accepted,
        rejected=total_rejected,
        late_accepted=total_late,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------
@app.get("/api/v1/windows/{window_type}")
async def get_windows(window_type: str) -> dict:
    """Return active windows with metrics for a given window type."""
    config: AppConfig = app.state.config
    aggregator: Aggregator = app.state.aggregator

    wt_config = None
    for wt in config.window_types:
        if wt.name == window_type:
            wt_config = wt
            break

    if wt_config is None:
        raise HTTPException(status_code=404, detail=f"Unknown window type: {window_type}")

    window_keys = await aggregator.get_active_windows(window_type)
    windows: list[dict] = []
    for wk in window_keys:
        metrics = await aggregator.get_window_metrics(wk, wt_config.size_seconds)
        redis_client = app.state.redis
        raw = await redis_client.hgetall(wk)
        if not raw:
            continue

        start_ts = int(raw.get(b"start_ts", 0))
        end_ts = int(raw.get(b"end_ts", 0))
        status = (raw.get(b"status", b"active")).decode()

        info = WindowInfo(
            window_key=wk,
            window_type=window_type,
            start_ts=start_ts,
            end_ts=end_ts,
            state=WindowState(status),
            metrics=metrics,
        )
        windows.append(info.model_dump())

    return {"window_type": window_type, "count": len(windows), "windows": windows}


@app.get("/api/v1/windows/{window_type}/history")
async def get_window_history(window_type: str) -> dict:
    """Return recently closed windows for a type (scan Redis for matching keys)."""
    config: AppConfig = app.state.config
    aggregator: Aggregator = app.state.aggregator
    redis_client = app.state.redis

    wt_config = None
    for wt in config.window_types:
        if wt.name == window_type:
            wt_config = wt
            break

    if wt_config is None:
        raise HTTPException(status_code=404, detail=f"Unknown window type: {window_type}")

    # Scan for all window keys of this type
    pattern = f"window:{window_type}:{wt_config.size_seconds}:*"
    closed: list[dict] = []
    async for key in redis_client.scan_iter(match=pattern.encode(), count=100):
        key_str = key.decode() if isinstance(key, bytes) else key
        raw = await redis_client.hgetall(key)
        if not raw:
            continue
        status = (raw.get(b"status", b"active")).decode()
        if status == "closed":
            metrics = await aggregator.get_window_metrics(key_str, wt_config.size_seconds)
            start_ts = int(raw.get(b"start_ts", 0))
            end_ts = int(raw.get(b"end_ts", 0))
            info = WindowInfo(
                window_key=key_str,
                window_type=window_type,
                start_ts=start_ts,
                end_ts=end_ts,
                state=WindowState.CLOSED,
                metrics=metrics,
            )
            closed.append(info.model_dump())

    return {"window_type": window_type, "count": len(closed), "windows": closed}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
@app.get("/api/v1/stats")
async def get_stats() -> dict:
    """Return system statistics."""
    config: AppConfig = app.state.config
    aggregator: Aggregator = app.state.aggregator
    start_time: int = app.state.start_time
    uptime = int(time.time()) - start_time

    active_counts: dict[str, int] = {}
    total_events = 0

    for wt in config.window_types:
        keys = await aggregator.get_active_windows(wt.name)
        active_counts[wt.name] = len(keys)
        for wk in keys:
            metrics = await aggregator.get_window_metrics(wk, wt.size_seconds)
            if metrics:
                total_events += metrics.count

    return {
        "uptime_seconds": uptime,
        "total_events": total_events,
        "active_windows": active_counts,
        "window_types": [wt.name for wt in config.window_types],
    }


# ---------------------------------------------------------------------------
# Dashboard & WebSocket
# ---------------------------------------------------------------------------
templates = Jinja2Templates(directory="src/templates")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the live analytics dashboard."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.websocket("/ws/dashboard")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time metric broadcasts."""
    ws_manager: ConnectionManager = app.state.ws_manager
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
