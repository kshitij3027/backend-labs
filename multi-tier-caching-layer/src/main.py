"""FastAPI application for the multi-tier caching layer.

This is the C15 wiring commit: the :func:`lifespan` context manager now builds
the **full object graph** (L1 + L2 Redis + Postgres pool + metrics + pattern
engine + single-flight + ``CacheManager`` + background ``warmer``) and attaches
every piece to ``app.state`` so the REST routers (and the dashboard/WS layers in
later commits) can reach them via ``src.api.dependencies``.

Startup ordering is the load-bearing detail here (plan Risk #2): the Redis L2
pool and the Postgres pool must be **connected before** the warmer task starts —
the warmer immediately replays recommendations through ``CacheManager.get``,
which touches both tiers. On shutdown we tear down in reverse: signal the warmer
to stop, cancel + await it (suppressing :class:`asyncio.CancelledError`), then
close the Redis client and the Postgres pool so no connections leak.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from src.api.routes_cache import router as cache_router
from src.api.routes_patterns import router as patterns_router
from src.api.routes_query import router as query_router
from src.cache_manager import CacheManager
from src.db.pool import apply_schema, create_pool
from src.l1_cache import L1Cache
from src.l2_redis import L2Redis
from src.metrics import Metrics
from src.patterns import PatternEngine
from src.settings import get_settings
from src.singleflight import SingleFlight
from src.warmer import Warmer
from src.websocket import ConnectionManager

logger = logging.getLogger("multi_tier_cache")


def _current_payload(app: FastAPI) -> dict:
    """Build the canonical ``/ws/metrics`` tick payload from ``app.state``.

    Reads the live ``metrics`` / ``patterns`` / ``l2`` collaborators off
    ``app.state`` so both the background broadcast loop and the per-connection
    immediate push emit an identical shape::

        {"type": "tick", "stats": <metrics.snapshot()>,
         "series": <metrics.series()>, "recommendations": [...],
         "degraded": <l2.degraded>}
    """
    metrics = app.state.metrics
    patterns = app.state.patterns
    l2 = app.state.l2
    return {
        "type": "tick",
        "stats": metrics.snapshot(),
        "series": metrics.series(),
        "recommendations": patterns.recommendations(10),
        "degraded": l2.degraded,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the object graph, start the warmer, and tear everything down.

    The build order matters: L2 (Redis) and the Postgres pool are connected
    **before** the background warmer task is created, because the warmer's first
    sweep replays recommendations through ``CacheManager.get`` which reads both
    tiers. On shutdown we reverse: stop + cancel + await the warmer, then close
    Redis and Postgres.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    # --- in-process L1 -------------------------------------------------------
    l1 = L1Cache(max_size=settings.l1_max_size, ttl=settings.l1_ttl)

    # --- L2 (Redis) — connect BEFORE the warmer ------------------------------
    l2 = L2Redis(
        settings.effective_redis_url,
        ttl_seconds=settings.l2_ttl_seconds,
        timeout=settings.l2_timeout,
        compress=settings.l2_compress,
    )
    try:
        await l2.connect()
    except Exception:  # noqa: BLE001 — fail-soft: L2 degrades, app still serves
        logger.exception("L2 (Redis) connect failed; continuing degraded")

    # --- Postgres pool (L3 + slow backend) — connect BEFORE the warmer -------
    pg_pool = await create_pool(settings.database_url)
    # Idempotent: db-init also applies this, but applying again is safe and makes
    # the in-process test path (which skips db-init) self-sufficient.
    await apply_schema(pg_pool)

    # --- observability + intelligence ---------------------------------------
    metrics = Metrics(
        degradation_threshold=settings.degradation_hit_rate_threshold,
        history_points=settings.dashboard_points,
    )
    patterns = PatternEngine(
        history_size=settings.pattern_history_size,
        freq_weight=settings.pattern_freq_weight,
        recency_weight=settings.pattern_recency_weight,
        cost_weight=settings.pattern_cost_weight,
        recency_half_life_seconds=settings.pattern_recency_half_life_seconds,
    )
    singleflight = SingleFlight()

    # --- read-through keystone ----------------------------------------------
    cache_manager = CacheManager(
        l1=l1,
        l2=l2,
        pg_pool=pg_pool,
        metrics=metrics,
        patterns=patterns,
        singleflight=singleflight,
        time_bucket_seconds=settings.time_bucket_seconds,
        backend_delay_ms=settings.backend_delay_ms,
        l2_ttl_seconds=settings.l2_ttl_seconds,
        l2_compress=settings.l2_compress,
    )

    # --- proactive warmer ----------------------------------------------------
    warmer = Warmer(
        cache_manager,
        patterns,
        interval_seconds=settings.warmer_interval_seconds,
        top_n=settings.warmer_top_n,
    )

    # --- dashboard WebSocket fan-out ----------------------------------------
    ws_manager = ConnectionManager()

    # Attach the whole graph so dependencies / routers can reach it.
    app.state.settings = settings
    app.state.l1 = l1
    app.state.l2 = l2
    app.state.pg_pool = pg_pool
    app.state.metrics = metrics
    app.state.patterns = patterns
    app.state.cache_manager = cache_manager
    app.state.warmer = warmer
    app.state.ws_manager = ws_manager

    # Canonical per-tick payload builder, sharing the shape used by the
    # ``/ws/metrics`` endpoint's immediate push (see ``_current_payload``).
    def _ws_payload() -> dict:
        return {
            "type": "tick",
            "stats": metrics.snapshot(),
            "series": metrics.series(),
            "recommendations": patterns.recommendations(10),
            "degraded": l2.degraded,
        }

    # Start the background warmer AFTER both tiers are connected.
    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    warmer_task = asyncio.create_task(warmer.run(stop_event))
    app.state.warmer_task = warmer_task

    # Start the metrics broadcast loop AFTER the warmer. It pushes a tick to
    # every connected dashboard every ``ws_push_interval_seconds`` and sleeps on
    # the shared stop_event so shutdown wakes it immediately. Each iteration
    # isolates exceptions so a transient broadcast failure never kills the loop.
    async def _broadcast_loop() -> None:
        while not stop_event.is_set():
            try:
                await ws_manager.broadcast(_ws_payload())
            except Exception:  # noqa: BLE001 — never let one tick kill the loop
                logger.exception("metrics broadcast tick failed")
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.ws_push_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    broadcast_task = asyncio.create_task(_broadcast_loop())
    app.state.broadcast_task = broadcast_task

    logger.info(
        "multi-tier caching layer starting on %s:%s",
        settings.api_host,
        settings.api_port,
    )

    try:
        yield
    finally:
        # Reverse-order teardown: stop both background tasks, then close tiers.
        stop_event.set()
        warmer_task.cancel()
        broadcast_task.cancel()
        with suppress(asyncio.CancelledError):
            await warmer_task
        with suppress(asyncio.CancelledError):
            await broadcast_task
        await l2.close()
        await pg_pool.close()
        logger.info("multi-tier caching layer shutdown")


app = FastAPI(title="Multi-Tier Caching Layer", lifespan=lifespan)
app.include_router(query_router)
app.include_router(cache_router)
app.include_router(patterns_router)


@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    """Stream live cache metrics to a dashboard client.

    On connect the client receives an immediate snapshot (so the dashboard
    paints without waiting for the next tick); thereafter the background
    broadcast loop in :func:`lifespan` pushes a tick every
    ``ws_push_interval_seconds``. We loop on ``receive_text`` purely to detect
    the client going away, unregistering it on :class:`WebSocketDisconnect`.
    """
    mgr = websocket.app.state.ws_manager
    await mgr.connect(websocket)
    try:
        # Immediate push so a freshly connected dashboard renders at once.
        await mgr.send_personal(websocket, _current_payload(websocket.app))
        while True:
            await websocket.receive_text()  # keep-alive; client messages ignored
    except WebSocketDisconnect:
        mgr.disconnect(websocket)


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
