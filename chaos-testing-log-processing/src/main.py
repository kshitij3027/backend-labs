"""FastAPI app factory + lifespan wiring for the chaos testing framework.

C6: the lifespan now boots a :class:`SystemMonitor` that polls host +
per-container metrics on the configured cadence. The monitor instance is
exposed in two ways:

- ``app.state.monitor`` --- normal FastAPI dependency-injection path.
- ``src.monitoring.system_monitor.get_monitor()`` --- module-level
  singleton used by the SafetySupervisor (C15) and WebSocket broadcaster
  (C14) where threading the instance through the call site would be
  awkward.

A temporary ``/internal/metrics-snapshot`` endpoint is exposed so the C6
E2E test can confirm metrics are actually being collected. It graduates
into the proper REST surface in C13.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config.settings import get_settings
from .docker_client.client import DockerClient
from .monitoring.system_monitor import (
    SystemMonitor,
    get_monitor,
    set_monitor,
)
from .persistence.repo import create_all_tables, make_engine, make_sessionmaker


logger = logging.getLogger("chaos.framework")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """App lifespan: boot DockerClient + SystemMonitor; tear down on shutdown."""
    settings = get_settings()
    docker_client = DockerClient(
        allowlist=settings.target_allowlist,
        socket_path=settings.docker_socket_path,
    )

    db_engine = make_engine(settings.database_url)
    await create_all_tables(db_engine)
    db_sessionmaker = make_sessionmaker(db_engine)
    app.state.db_engine = db_engine
    app.state.db_sessionmaker = db_sessionmaker

    # Probe ``/health`` on every allowlisted target except redis (no HTTP).
    monitor = SystemMonitor(
        docker_client=docker_client,
        interval_seconds=settings.metrics_collection_interval_seconds,
        history_size=settings.metrics_history_size,
        target_health_paths={
            name: "/health"
            for name in settings.target_allowlist
            if name != "redis"
        },
    )
    set_monitor(monitor)
    await monitor.start()

    app.state.settings = settings
    app.state.docker_client = docker_client
    app.state.monitor = monitor

    logger.info(
        "framework lifespan started (interval=%ss history=%s)",
        settings.metrics_collection_interval_seconds,
        settings.metrics_history_size,
    )

    try:
        yield
    finally:
        await monitor.stop()
        docker_client.close()
        await db_engine.dispose()
        set_monitor(None)
        logger.info("framework lifespan stopped")


app = FastAPI(
    title="Chaos Testing Framework",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> JSONResponse:
    """Liveness probe used by Docker HEALTHCHECK and compose."""
    monitor = get_monitor()
    payload = {
        "status": "ok",
        "version": "0.1.0",
        "monitor_running": monitor is not None and monitor.history_size() >= 0,
    }
    return JSONResponse(payload)


@app.get("/internal/metrics-snapshot")
async def metrics_snapshot() -> JSONResponse:
    """TEMP debug endpoint: returns the latest snapshot + history length.

    Will be folded into the proper REST surface in C13. Kept here in C6
    so the test agent can confirm collection is actually happening.
    """
    monitor = get_monitor()
    if monitor is None:
        return JSONResponse({"snapshot": None, "history_size": 0})
    snap = monitor.snapshot()
    return JSONResponse(
        {
            "snapshot": snap.model_dump(mode="json") if snap is not None else None,
            "history_size": monitor.history_size(),
        }
    )
