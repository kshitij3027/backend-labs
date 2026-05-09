"""REST endpoints (GET only in this commit; POSTs land in Commit 11)."""
from __future__ import annotations
import asyncio
import time

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

from src.api.models import (
    HealthResponse,
    MetricsResponse,
    ProcessLogsRequest,
    SimulateFailuresRequest,
)
from src.api.prometheus import CONTENT_TYPE_LATEST

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


@router.post("/api/process/logs")
async def process_logs(payload: ProcessLogsRequest, request: Request):
    processor = request.app.state.processor
    result = await processor.process_batch(payload.count)
    return result


async def _failure_simulation(service, failure_rate: float, duration: int):
    """Background task: enable injector for ``duration`` seconds, then reset."""
    service.injector.set_failure_rate(failure_rate)
    try:
        await asyncio.sleep(duration)
    finally:
        service.injector.set_failure_rate(0.0)


@router.post("/api/simulate/failures")
async def simulate_failures(
    payload: SimulateFailuresRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    services = request.app.state.services
    if payload.target not in services:
        raise HTTPException(
            status_code=400,
            detail=f"unknown target '{payload.target}'. valid: {sorted(services.keys())}",
        )
    background_tasks.add_task(
        _failure_simulation, services[payload.target], payload.failure_rate, payload.duration,
    )
    return {
        "simulating": payload.target,
        "duration": payload.duration,
        "failure_rate": payload.failure_rate,
    }


@router.post("/api/reset")
async def reset_breakers(request: Request):
    registry = request.app.state.registry
    services = request.app.state.services
    await registry.reset_all()
    for svc in services.values():
        svc.injector.reset()
    return {"reset": True, "circuits": sorted(registry.names())}


@router.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket):
    manager = websocket.app.state.manager
    await manager.connect(websocket)
    try:
        # Push an immediate snapshot so a freshly-connected client doesn't wait 2s.
        from src.api.websocket import build_metrics_snapshot
        snapshot = build_metrics_snapshot(websocket.app.state.registry, websocket.app.state.processor)
        await websocket.send_json(snapshot)
        # Wait for client disconnect.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)


@router.get("/metrics")
async def prometheus_metrics(request: Request):
    """Prometheus exposition. Refresh gauges from registry snapshot, then emit."""
    registry = request.app.state.registry
    processor = request.app.state.processor
    prom = request.app.state.prometheus
    prom.update_from_snapshot(registry.metrics_snapshot(), processor.get_processing_stats())
    return Response(content=prom.render(), media_type=CONTENT_TYPE_LATEST)


@router.get("/api/alerts")
async def alerts(request: Request):
    alerter = request.app.state.alerter
    return {"events": alerter.events()}
