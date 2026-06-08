"""FastAPI application for the Bloom Filter Log Membership service.

C2 skeleton: the :func:`lifespan` context manager resolves settings, configures
logging, and publishes the settings on ``app.state``; the only route is the
``GET /health`` liveness probe that Docker's HEALTHCHECK (and the compose wait
loops) poll. Later commits grow this module in place: C8 wires the per-log-type
filter manager plus ``/logs/add`` / ``/logs/query`` / ``/stats`` and the
snapshot/rotation background tasks, C9 adds the ``/demo`` endpoints, C10 the
``/pipeline`` two-tier endpoints, and C11 the ``/sessions`` endpoints.

The service always runs a SINGLE uvicorn worker: all filter state lives
in-process, so multiple workers would each hold a divergent copy of every
filter and answer queries inconsistently.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.settings import get_settings

logger = logging.getLogger("bloom_filter_log_membership")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Resolve settings, configure logging, and log startup/shutdown.

    Kept intentionally thin in C2. Later commits extend it to: build the
    :class:`FilterManager`, reload persisted snapshots from ``data_dir``, start
    the periodic snapshot + rotation tasks after the graph is published on
    ``app.state``, and run a final save on the way out.
    """
    settings = get_settings()
    # basicConfig accepts level names ("INFO") as well as numeric levels.
    logging.basicConfig(level=settings.log_level)
    app.state.settings = settings

    logger.info(
        "bloom-filter-log-membership starting on %s:%s (data_dir=%s)",
        settings.api_host,
        settings.api_port,
        settings.data_dir,
    )
    try:
        yield
    finally:
        logger.info("bloom-filter-log-membership shutdown")


app = FastAPI(title="Bloom Filter Log Membership API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker's HEALTHCHECK and the compose wait loops."""
    return {"status": "healthy"}


if __name__ == "__main__":
    # Convenience entrypoint for `python -m src.api`; Docker runs uvicorn directly.
    settings = get_settings()
    uvicorn.run(
        "src.api:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
