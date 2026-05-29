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
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.api.routes_load import router as load_router
from src.api.routes_metrics import router as metrics_router
from src.api.routes_optimizer import router as optimizer_router
from src.batcher import AdaptiveBatcher
from src.settings import get_settings
from src.websocket import ConnectionManager

logger = logging.getLogger("adaptive_batcher")


def _ws_payload(batcher) -> dict:
    """Build the JSON payload broadcast to dashboard clients each tick.

    Bundles the latest snapshot, the current optimizer status, and the rolling
    chart series into one envelope so a client can paint the whole dashboard from
    a single message. Pydantic models are dumped with ``mode="json"`` so enums
    (e.g. :class:`~src.models.OptimizerState`) serialise to their values.

    Args:
        batcher: The live :class:`~src.batcher.AdaptiveBatcher`.

    Returns:
        A JSON-serialisable dict tagged ``"type": "tick"``.
    """
    snap = batcher.latest_snapshot()
    return {
        "type": "tick",
        "snapshot": snap.model_dump(mode="json") if snap is not None else None,
        "status": batcher.status().model_dump(mode="json"),
        "series": batcher.metrics_series(get_settings().dashboard_points),
    }


async def optimization_loop(app: FastAPI) -> None:
    """Drive the control loop forever, one :meth:`AdaptiveBatcher.tick` per cycle.

    Each iteration reads the live cadence from ``app.state.loop_interval`` (so a
    config change retunes it without a restart), ticks the batcher with that
    interval, broadcasts the fresh optimizer state to every ``/ws/metrics``
    client, then sleeps for the same interval. The tick *and* the broadcast share
    one ``try`` block: a failing tick or a failing broadcast is logged and
    swallowed so a single bad cycle (or a misbehaving client) never kills the
    loop; an :class:`asyncio.CancelledError` (raised on shutdown) is re-raised so
    the task can unwind cleanly.

    Args:
        app: The running application, used to reach ``app.state.batcher``, the
            current ``app.state.loop_interval``, and ``app.state.ws_manager``.
    """
    while True:
        interval = app.state.loop_interval
        try:
            app.state.batcher.tick(interval=interval)
            await app.state.ws_manager.broadcast(_ws_payload(app.state.batcher))
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
    app.state.ws_manager = ConnectionManager()  # dashboard fan-out, before the loop
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

# Live dashboard: vanilla HTML + vendored Chart.js, served same-origin. The
# template/static dirs are copied into the image (see Dockerfile) and read at
# runtime relative to the working directory.
templates = Jinja2Templates(directory="dashboard/templates")
app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the live optimizer dashboard (it streams from ``/ws/metrics``)."""
    return templates.TemplateResponse(
        request,
        "index.html",
        {"dashboard_points": get_settings().dashboard_points},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker's HEALTHCHECK and the e2e wait loop."""
    return {"status": "healthy"}


@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    """Stream the optimizer state + metrics to a dashboard client.

    On connect the current state is pushed immediately (so a fresh client paints
    without waiting a full tick); thereafter the background
    :func:`optimization_loop` broadcasts a fresh payload on every cycle. Inbound
    client messages are ignored — the receive loop exists only to detect a
    disconnect and unregister the socket so it stops receiving broadcasts.
    """
    manager = websocket.app.state.ws_manager
    await manager.connect(websocket)
    # Push the current state immediately so a fresh client paints without waiting a full tick.
    await manager.send_personal(websocket, _ws_payload(websocket.app.state.batcher))
    try:
        while True:
            await websocket.receive_text()  # keep the connection open; client msgs ignored
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:  # noqa: BLE001 — any socket error: drop the client cleanly
        manager.disconnect(websocket)


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
