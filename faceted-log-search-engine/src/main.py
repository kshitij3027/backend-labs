"""FastAPI application entrypoint.

Wires the SQLite connection into ``app.state.db`` for the lifetime
of the process via a lifespan context manager, exposes ``/health``,
and runs uvicorn when executed directly. Later commits mount API
routers (logs, search, stats, ui) onto this same app instance.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.api import logs as logs_api
from src.config import settings
from src.storage import sqlite_store

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open + migrate sqlite at startup, close at shutdown."""
    # Ensure DB parent directory exists (volume mount may be empty).
    parent = os.path.dirname(settings.db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    db = await sqlite_store.connect(settings.db_path)
    await sqlite_store.migrate(db)
    app.state.db = db
    logger.info("startup complete db_path=%s", settings.db_path)
    try:
        yield
    finally:
        await sqlite_store.close(db)
        logger.info("shutdown complete")


app = FastAPI(
    title="Faceted Log Search Engine",
    version="0.1.0",
    description="Multi-dimensional faceted search for structured logs.",
    lifespan=lifespan,
)

# Router wiring. /health stays on the app itself so the healthcheck
# endpoint is stable regardless of future router reorganizations.
app.include_router(logs_api.router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness/readiness probe.

    Redis reachability is reported as the configured URL for now;
    a real ping is added in C4 when the Redis cache client lands.
    """
    return {
        "status": "ok",
        "db": "connected",
        "redis_url": settings.redis_url,
    }


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
