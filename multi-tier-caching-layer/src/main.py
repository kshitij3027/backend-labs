"""FastAPI application for the multi-tier caching layer.

This is the C2 runtime skeleton: a minimal, cleanly extensible app that boots
with **no external services** (no Redis, no Postgres needed for ``/health``).

The :func:`lifespan` context manager is intentionally trivial right now — it
only stashes :class:`~src.settings.Settings` on ``app.state`` and yields. Later
commits build the real object graph here (Redis L2 pool, Postgres pool, the
``CacheManager``, the heuristic ``patterns`` engine, ``metrics``, and the
background ``warmer`` task) — see the marked extension point below.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.settings import get_settings

logger = logging.getLogger("multi_tier_cache")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup/shutdown.

    For C2 this only loads settings onto ``app.state`` so the rest of the app
    (and tests driving the app through its lifespan) can reach them. Keep this
    body trivially extensible.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    app.state.settings = settings
    logger.info(
        "multi-tier caching layer starting on %s:%s",
        settings.api_host,
        settings.api_port,
    )

    # --- EXTENSION POINT (later commits) ---------------------------------
    # Build the object graph here and attach it to ``app.state``:
    #   * connect the Redis L2 pool and the Postgres pool (connect BEFORE the
    #     warmer task starts),
    #   * construct ``CacheManager`` (L1 + L2 + L3 + backend + materializer),
    #     the heuristic ``patterns`` engine, the ``metrics`` aggregator, and the
    #     ``ConnectionManager`` for the dashboard WebSocket,
    #   * start the background ``warmer`` task (and the WS broadcast loop).
    # On shutdown: signal stop, cancel/await background tasks, then close the
    # Redis and Postgres pools cleanly.
    # ---------------------------------------------------------------------

    try:
        yield
    finally:
        logger.info("multi-tier caching layer shutdown")


app = FastAPI(title="Multi-Tier Caching Layer", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker's HEALTHCHECK and the e2e wait loop."""
    return {"status": "healthy"}


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
