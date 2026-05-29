"""FastAPI application + the background adaptive-optimization loop.

This is the C7 runtime glue. The heavy lifting lives in
:class:`~src.batcher.AdaptiveBatcher`; here we only:

* construct the batcher once at startup and stash it (plus settings and the
  live loop cadence) on ``app.state``,
* run :meth:`AdaptiveBatcher.tick` on a timer inside a resilient background
  task that survives per-tick exceptions and shuts down cleanly on cancel,
* mount the thin REST routers and a ``/health`` probe.

The loop reads its interval from ``app.state.loop_interval`` on every iteration,
so ``POST /api/optimizer/config`` can retune the cadence live (see
:mod:`src.api.routes_optimizer`).
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.api.routes_load import router as load_router
from src.api.routes_metrics import router as metrics_router
from src.api.routes_optimizer import router as optimizer_router
from src.batcher import AdaptiveBatcher
from src.settings import get_settings

logger = logging.getLogger("adaptive_batcher")


async def optimization_loop(app: FastAPI) -> None:
    """Drive the control loop forever, one :meth:`AdaptiveBatcher.tick` per cycle.

    Each iteration reads the live cadence from ``app.state.loop_interval`` (so a
    config change retunes it without a restart), ticks the batcher with that
    interval, then sleeps for the same interval. A failing tick is logged and
    swallowed so a single bad cycle never kills the loop; an
    :class:`asyncio.CancelledError` (raised on shutdown) is re-raised so the task
    can unwind cleanly.

    Args:
        app: The running application, used to reach ``app.state.batcher`` and the
            current ``app.state.loop_interval``.
    """
    while True:
        interval = app.state.loop_interval
        try:
            app.state.batcher.tick(interval=interval)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — never let one bad tick kill the loop
            logger.exception("optimization tick failed; continuing")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the batcher, start the background loop, and tear it down on exit."""
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    app.state.settings = settings
    app.state.batcher = AdaptiveBatcher()  # constructs components, primes psutil
    app.state.loop_interval = settings.optimization_interval
    app.state.optimization_task = asyncio.create_task(optimization_loop(app))
    logger.info(
        "adaptive batcher starting on %s:%s (interval=%.3gs)",
        settings.api_host,
        settings.api_port,
        settings.optimization_interval,
    )

    try:
        yield
    finally:
        task = app.state.optimization_task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("adaptive batcher shutdown")


app = FastAPI(title="Adaptive Batching Engine Optimizer", lifespan=lifespan)
app.include_router(metrics_router)
app.include_router(optimizer_router)
app.include_router(load_router)


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
