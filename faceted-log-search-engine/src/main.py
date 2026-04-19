"""FastAPI application entrypoint.

Wires the SQLite connection into ``app.state.db`` and the Redis
client into ``app.state.redis`` for the lifetime of the process via
a lifespan context manager. Exposes ``/health`` (with live DB and
Redis probes) and runs uvicorn when executed directly. Later
commits mount API routers (ui) onto this same app instance.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.api import logs as logs_api
from src.api import search as search_api
from src.api import stats as stats_api
from src.api import ui as ui_api
from src.config import settings
from src.storage import redis_cache, sqlite_store

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open DB pool + Redis at startup, close both at shutdown.

    Uses ``AsyncSqlitePool`` so the search hot path can fan out across
    multiple read connections (aiosqlite serializes per-Connection, so
    one shared conn turns 100-way concurrency into a queue). Writes
    still go through a single dedicated connection to avoid SQLite's
    "database is locked" window when two writers race.
    """
    # Ensure DB parent directory exists (volume mount may be empty).
    parent = os.path.dirname(settings.db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    pool = sqlite_store.AsyncSqlitePool(settings.db_path, read_size=8)
    await pool.open()
    app.state.db_pool = pool
    # Keep ``app.state.db`` pointing at the write connection so the
    # handful of legacy call sites (ingest, generate, analyze, tests
    # that reuse the lifespan) keep working unchanged.
    app.state.db = pool.write

    # Redis is lazy-connected — ``connect`` does not ping, so even if
    # Redis is down the server still comes up. Individual requests
    # fall through to the compute path via ``get_or_compute``.
    app.state.redis = await redis_cache.connect(settings.redis_url)
    reachable = await redis_cache.ping(app.state.redis)
    logger.info(
        "startup complete db_path=%s redis_url=%s redis_reachable=%s",
        settings.db_path,
        settings.redis_url,
        reachable,
    )
    try:
        yield
    finally:
        await pool.close()
        # ``aclose`` is best-effort; swallow errors so shutdown never
        # blocks on a transient Redis issue.
        if app.state.redis is not None:
            with suppress(Exception):
                await app.state.redis.aclose()
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
app.include_router(search_api.router)
app.include_router(stats_api.router)

# Static assets for the dashboard. Mount before including the UI
# router so ``/static/*`` always routes to StaticFiles regardless of
# future ``/`` catch-all changes.
app.mount("/static", StaticFiles(directory="src/static"), name="static")
app.include_router(ui_api.router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness/readiness probe.

    DB is considered primary — if it's up the overall status is
    ``ok`` even when Redis is reported as ``down``. This keeps the
    app healthy from the Docker/k8s healthcheck perspective when the
    cache layer is transiently unavailable (cache-aside fallback
    handles the rest at request time).
    """
    redis_client = getattr(app.state, "redis", None)
    redis_ok = await redis_cache.ping(redis_client)
    return {
        "status": "ok",
        "db": "connected",
        "redis": "ok" if redis_ok else "down",
        "redis_url": settings.redis_url,
    }


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)
