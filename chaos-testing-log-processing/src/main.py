"""Chaos Testing Framework — FastAPI entry point.

C1 scaffold: only `/health` is wired up. Lifespan hooks (engine start,
broadcaster task, DB init) land in later commits but the context manager
is already plumbed so subsequent commits don't churn the app wiring.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """App lifespan — startup/teardown wiring lands in later commits."""
    # Startup: future commits hook the SystemMonitor, ExperimentEngine,
    # SafetySupervisor, WebSocket broadcaster, and SQLite init here.
    yield
    # Shutdown: future commits cancel background tasks and flush state here.


app = FastAPI(
    title="Chaos Testing Framework",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> JSONResponse:
    """Liveness probe used by Docker HEALTHCHECK and compose."""
    return JSONResponse({"status": "ok", "version": "0.1.0"})
