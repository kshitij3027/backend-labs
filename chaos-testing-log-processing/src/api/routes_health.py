"""Liveness / readiness."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    monitor = getattr(request.app.state, "monitor", None)
    docker = getattr(request.app.state, "docker_client", None)
    db = getattr(request.app.state, "db_engine", None)

    return JSONResponse(
        {
            "status": "ok",
            "version": "0.1.0",
            "monitor_running": monitor is not None and monitor.history_size() >= 0,
            "docker_connected": docker is not None,
            "db_connected": db is not None,
        }
    )
