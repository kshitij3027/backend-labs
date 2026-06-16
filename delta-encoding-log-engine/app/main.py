"""FastAPI application entry point for the delta-encoding log engine.

This is the wiring commit: a single long-lived FastAPI process on **port 8080** that
owns the whole engine in memory. The :func:`lifespan` context manager builds the full
``app.state`` object graph — runtime :class:`~app.settings.Settings`, the live
:class:`~app.metrics.MetricsRegistry`, and the in-memory
:class:`~app.store.SegmentStore` (delta codec + byte accounting) — **before** ``yield``
so every request handler can reach a fully-formed graph through ``request.app.state``.
The data graph is in-process and garbage-collected with the app; the one piece of
teardown is the dashboard's background broadcast loop, which the lifespan stops and
awaits after ``yield`` (see below).

The live monitoring dashboard (``GET /``, ``/static/*``, ``WS /ws``) is served from
:mod:`app.dashboard`: a single background loop fans a stats tick out to every connected
dashboard every ``dashboard_refresh_ms``, built in-process (no HTTP self-call).

**Single process, single worker (see *plan.md → Architecture*).** The store and metrics
live in process memory, so the deployment is intentionally one uvicorn worker — multiple
workers would each hold a divergent copy of the batch and the counters. The
sync-vs-async handler split that keeps the event loop responsive lives in :mod:`app.api`.

The api router carries full paths (``/health`` at the root, the rest under ``/api/...``)
and is included with **no prefix**, so the final routes are exactly ``/health``,
``/api/generate``, ``/api/compress``, ``/api/reconstruct``, ``/api/logs``,
``/api/logs/{index}``, ``/api/stats``, ``/api/reset``.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.analyzer import PatternAnalyzer
from app.api import router as api_router
from app.dashboard import ConnectionManager, broadcast_loop
from app.dashboard import router as dashboard_router
from app.encoders import EncoderConfig
from app.metrics import MetricsRegistry
from app.reconstruct import ReconstructionCache
from app.settings import get_settings
from app.store import SegmentStore

# Project root (…/delta-encoding-log-engine), which is ``/app`` inside the container.
# Resolved from this module's file so the static mount works regardless of CWD.
BASE_DIR = Path(__file__).resolve().parent.parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the full ``app.state`` graph, start the dashboard loop, then tear it down.

    Resolves the cached settings, then constructs the metrics registry, the segment
    store (typed encoding active via ``EncoderConfig.all_on()``), and the bounded
    reconstruction cache, and publishes all four on ``app.state`` so the api handlers can
    read them. The store's keyframe interval, baseline, gzip-deltas flag, and the cache
    size come straight from configuration, keeping the engine's behaviour env-driven.

    **Dashboard fan-out.** After the data graph is published and **before** ``yield``, a
    single :class:`~app.dashboard.ConnectionManager`, an :class:`asyncio.Event` stop
    signal, and the one background :func:`~app.dashboard.broadcast_loop` task are created
    on ``app.state``. The loop reads them from there and pushes a tick to every connected
    dashboard every ``dashboard_refresh_ms``. On shutdown (after ``yield``) the stop event
    is set so the loop exits its sleep promptly, the task is awaited (its
    :class:`asyncio.CancelledError` suppressed), and any sockets still open are closed —
    so there are no pending-task warnings.
    """
    settings = get_settings()
    app.state.settings = settings
    app.state.metrics = MetricsRegistry()
    app.state.store = SegmentStore(
        keyframe_interval=settings.keyframe_interval,
        baseline=settings.delta_baseline,
        encoder_config=EncoderConfig.all_on(),  # typed encoding active
        gzip_deltas=settings.gzip_deltas,
    )
    # Bounded LRU in front of single-entry reconstruction (size 0 disables it).
    app.state.recon_cache = ReconstructionCache(settings.reconstruct_cache_size)

    # Thin, READ-ONLY adaptive recommender: a sliding-window churn observer that only
    # *reports* a recommended keyframe interval + compression mode (surfaced additively
    # in /api/stats and on the WS tick). It holds no encoder reference and never mutates
    # the store/codec — compression output is byte-identical with or without it.
    app.state.analyzer = PatternAnalyzer(
        window=settings.analyzer_window,
        current_interval=settings.keyframe_interval,
    )

    # Dashboard WebSocket fan-out: one manager, one stop event, one broadcast loop.
    # Created AFTER the data graph above (the loop reads it on its first tick) and BEFORE
    # yield (so the server is serving with the loop already ticking).
    app.state.ws_manager = ConnectionManager()
    app.state.ws_stop = asyncio.Event()
    app.state.ws_task = asyncio.create_task(broadcast_loop(app))

    try:
        yield
    finally:
        # Cooperative shutdown: signal the loop, await it (it exits its wait promptly),
        # then close any sockets still attached so nothing is left dangling.
        app.state.ws_stop.set()
        try:
            await app.state.ws_task
        except asyncio.CancelledError:
            pass
        for websocket in list(app.state.ws_manager._connections):
            try:
                await websocket.close()
            except Exception:  # noqa: BLE001 — best-effort close on shutdown.
                pass


app = FastAPI(title="Delta Encoding Log Engine", lifespan=lifespan)

# REST API + /health. Full paths live on the routes, so no prefix here.
app.include_router(api_router)

# Live monitoring dashboard: the single page (GET /) and fan-out WebSocket (WS /ws).
# Its routes carry full paths too, so no prefix here either.
app.include_router(dashboard_router)

# Serve the dashboard's static assets (JS / CSS, and the vendored Chart.js added next
# commit). BASE_DIR resolves from this module so the mount works from /app in the
# container; the Dockerfile COPYs the whole ``dashboard/`` tree so it ships with the image.
app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "dashboard" / "static")),
    name="static",
)
