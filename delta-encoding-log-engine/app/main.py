"""FastAPI application entry point for the delta-encoding log engine.

This is the wiring commit: a single long-lived FastAPI process on **port 8080** that
owns the whole engine in memory. The :func:`lifespan` context manager builds the full
``app.state`` object graph — runtime :class:`~app.settings.Settings`, the live
:class:`~app.metrics.MetricsRegistry`, and the in-memory
:class:`~app.store.SegmentStore` (delta codec + byte accounting) — **before** ``yield``
so every request handler can reach a fully-formed graph through ``request.app.state``.
There is no teardown work beyond ``yield`` (all state is in-process and garbage-collected
with the app).

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

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router as api_router
from app.encoders import EncoderConfig
from app.metrics import MetricsRegistry
from app.reconstruct import ReconstructionCache
from app.settings import get_settings
from app.store import SegmentStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the full ``app.state`` graph before serving; nothing to tear down.

    Resolves the cached settings, then constructs the metrics registry, the segment
    store (typed encoding active via ``EncoderConfig.all_on()``), and the bounded
    reconstruction cache, and publishes all four on ``app.state`` so the api handlers can
    read them. The store's keyframe interval, baseline, gzip-deltas flag, and the cache
    size come straight from configuration, keeping the engine's behaviour env-driven.
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
    yield


app = FastAPI(title="Delta Encoding Log Engine", lifespan=lifespan)

# REST API + /health. Full paths live on the routes, so no prefix here.
app.include_router(api_router)

# NOTE: the live monitoring dashboard (GET /, /static/*, WS /ws) is added in a later
# commit via its own router/static mount. Intentionally not wired yet — this commit
# ships the REST API only.
