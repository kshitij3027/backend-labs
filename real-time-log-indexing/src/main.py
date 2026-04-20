"""FastAPI app + lifespan wiring for the real-time log indexing engine.

This module is the process entrypoint. It:

* builds the :class:`LogTokenizer`, :class:`InvertedIndex`, and
  :class:`RedisStreamConsumer` at startup;
* rehydrates any on-disk segments so search survives restarts;
* spawns the consumer as a background task owned by the event loop;
* serves a placeholder dashboard at ``GET /`` (full UI ships in
  Commit 10), the ``/health`` probe, and the ``/api/stats`` endpoint;
* on shutdown, signals the consumer to stop, flushes the current
  segment, awaits the consumer task (with a bounded timeout), and
  closes the Redis client.

Testability
-----------

The lifespan is intentionally kept *thin* — all component
construction lives in :func:`build_app_state`, which returns the
collection of objects that end up on ``app.state``. Tests construct
the app with an overridden :class:`Settings` via :func:`build_app`
and let the real lifespan run against a tmp segment directory. If a
future test needs finer control it can monkeypatch
:func:`build_app_state`.

Redis robustness
----------------

If Redis is unreachable at startup, we log a warning but keep the
app up — the consumer has its own exponential backoff and will
reconnect once Redis comes back. ``/health`` reports ``degraded`` in
that window. This matches the operational contract in the plan
(tests may start the app with Redis down).
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import redis.asyncio as redis_async
import redis.exceptions
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.api.routes import router
from src.api.websocket import ConnectionManager, register_ws_routes
from src.config import Settings, settings as default_settings
from src.index.inverted_index import InvertedIndex
from src.index.merger import merge_loop
from src.index.tokenizer import LogTokenizer
from src.logging_setup import setup_logging
from src.sample_data import LEVELS, SERVICES
from src.stream.redis_consumer import RedisStreamConsumer


logger = logging.getLogger(__name__)


# Maximum time we'll wait for the background consumer task to unwind
# during shutdown before cancelling it outright. Kept short so `make
# down` / pytest teardown doesn't stall on a stuck XREADGROUP.
_CONSUMER_SHUTDOWN_TIMEOUT_S: float = 5.0

# Bound the wait for WebSocket background loops (stats broadcast +
# heartbeat) to wind down during shutdown. Their loops check the stop
# event at each interval boundary, so this only matters if a slow
# broadcast coroutine gets stuck mid-send.
_WS_TASK_SHUTDOWN_TIMEOUT_S: float = 3.0

# Cadence for the stats-broadcast loop. One second matches the dashboard
# polling rate that existed before WS landed, so toggling between the
# two transports is imperceptible.
_STATS_BROADCAST_INTERVAL_S: float = 1.0


# ---------------------------------------------------------------------------
# App state construction
# ---------------------------------------------------------------------------

async def build_app_state(
    settings: Settings, ws_manager: ConnectionManager | None = None
) -> dict:
    """Build every object the lifespan will stash on ``app.state``.

    Returns a dict of:

    * ``index``          — the :class:`InvertedIndex` with any on-disk
                           segments already rehydrated.
    * ``consumer``       — the :class:`RedisStreamConsumer` ready to
                           be started (``consumer.run()`` spawned by
                           the lifespan).
    * ``redis_client``   — the async Redis client the dashboard /
                           health probe use directly. ``None`` if the
                           initial connection attempt failed — the
                           consumer retries on its own.
    * ``redis_connected``— bool echo of whether ``redis_client`` is
                           usable right now; ``/health`` surfaces this
                           as ``degraded`` when False.
    * ``stop_event``     — shared event wired into the consumer so the
                           lifespan can tell it to wind down.
    * ``settings``       — the exact :class:`Settings` used to build
                           the rest, stashed so handlers/tests can
                           introspect.
    * ``ws_manager``     — the :class:`ConnectionManager` used by the
                           WebSocket endpoint and broadcast loops.

    If *ws_manager* is ``None`` a fresh one is created; the builder
    accepts an explicit instance so :func:`build_app` can register the
    ``/ws`` route against the same manager that later receives the
    index callback and broadcasts.

    Splitting this out keeps the lifespan readable (it just plumbs
    the dict into ``app.state``, spawns the consumer task, and yields)
    and makes the construction order trivial to unit-test.
    """
    tokenizer = LogTokenizer()
    disk_dir = Path(settings.disk_segment_dir)

    if ws_manager is None:
        ws_manager = ConnectionManager()

    # Wiring the WS broadcast as the index's on_new_document callback
    # means every successfully indexed document lands on every
    # connected dashboard without the index knowing anything about WS.
    index = InvertedIndex(
        settings,
        tokenizer,
        disk_dir=disk_dir,
        on_new_document=ws_manager.broadcast_new_document,
    )
    await index.load_from_disk()

    stop_event = asyncio.Event()
    consumer = RedisStreamConsumer(settings, index, stop_event=stop_event)

    # Attempt a best-effort Redis connection so ``/health`` can ping
    # directly. Failure here is non-fatal — the consumer's own reconnect
    # loop handles transient outages, and health flips back to ``ok``
    # as soon as the ping starts succeeding.
    redis_client: redis_async.Redis | None = None
    redis_connected = False
    try:
        redis_client = redis_async.from_url(
            settings.redis_url, decode_responses=False
        )
        # ``ping`` proves the URL is actually routable; ``from_url``
        # alone is lazy and will happily construct a client pointing
        # at a dead host.
        await redis_client.ping()
        redis_connected = True
        logger.info("redis reachable at %s", settings.redis_url)
    except (
        ConnectionError,
        TimeoutError,
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError,
        OSError,
    ) as exc:
        logger.warning(
            "redis unreachable at %s: %s; starting degraded",
            settings.redis_url,
            exc,
        )
        # Keep the client object around — the consumer loop will use
        # a fresh one of its own, but we hold a reference so ``/health``
        # can retry the ping on its next call if the broker comes back.
        redis_connected = False

    return {
        "index": index,
        "consumer": consumer,
        "redis_client": redis_client,
        "redis_connected": redis_connected,
        "stop_event": stop_event,
        "settings": settings,
        "ws_manager": ws_manager,
    }


# ---------------------------------------------------------------------------
# Stats broadcast loop
# ---------------------------------------------------------------------------

async def stats_broadcast_loop(
    app: FastAPI,
    stop_event: asyncio.Event,
    interval: float = _STATS_BROADCAST_INTERVAL_S,
) -> None:
    """Periodically broadcast the latest stats to every WS client.

    Mirrors the payload shape of ``GET /api/stats`` so the dashboard
    can use the same ``applyStats`` handler for both transport paths —
    the only difference is the trigger (1 Hz timer vs. push). We
    assemble the dict here rather than calling the FastAPI handler
    directly because the handler is request-scoped; duplicating the
    compose is cheaper than running a fake request through the router.
    """
    while not stop_event.is_set():
        try:
            index = getattr(app.state, "index", None)
            ws_manager: ConnectionManager | None = getattr(
                app.state, "ws_manager", None
            )
            if index is None or ws_manager is None:
                # Lifespan hasn't finished wiring yet — skip this tick.
                pass
            else:
                consumer = getattr(app.state, "consumer", None)
                started_at = getattr(app.state, "started_at", time.time())
                uptime = max(time.time() - started_at, 0.0)

                raw = index.stats()
                denom = max(uptime, 1.0)
                throughput = raw["docs_indexed"] / denom
                consumer_errors = (
                    getattr(consumer, "errors", 0) if consumer else 0
                )

                payload = {
                    "docs_indexed": raw["docs_indexed"],
                    "current_segment_docs": raw["current_segment_docs"],
                    "flushed_memory_segments": raw["flushed_memory_segments"],
                    "disk_segments": raw["disk_segments"],
                    "vocab_size": raw["vocab_size"],
                    "memory_bytes": raw["memory_bytes"],
                    "throughput_1m": throughput,
                    "ingest_lag": 0,
                    "query_p95_ms": 0.0,
                    "errors": consumer_errors + raw.get("errors", 0),
                    "uptime_s": uptime,
                }
                await ws_manager.broadcast_stats(payload)
        except Exception as exc:  # noqa: BLE001 — never kill the loop
            logger.warning("stats broadcast error: %s", exc)

        # Honour the stop event so shutdown doesn't wait a full
        # interval before the loop exits.
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan: build state, spawn consumer, tear down cleanly.

    The settings instance comes from ``app.state.settings`` if a test
    injected one via :func:`build_app`; otherwise we fall back to the
    module-level singleton. ``app.state.started_at`` is set *before*
    the yield so the very first ``/health`` call reports a sane
    uptime.
    """
    settings: Settings = getattr(app.state, "settings", default_settings)

    # Configure structured JSON logging once per process. Safe to call
    # repeatedly — ``setup_logging`` scrubs existing handlers first.
    setup_logging(settings.log_level)

    # The ws_manager was built in ``build_app`` so the ``/ws`` route
    # could be registered at route-time. Reuse it here — building a
    # second one would split the client set between the endpoint and
    # the index callback.
    ws_manager: ConnectionManager = getattr(app.state, "ws_manager", None)
    state = await build_app_state(settings, ws_manager=ws_manager)
    app.state.index = state["index"]
    app.state.consumer = state["consumer"]
    app.state.redis_client = state["redis_client"]
    app.state.redis_connected = state["redis_connected"]
    app.state.stop_event = state["stop_event"]
    app.state.settings = settings
    app.state.ws_manager = state["ws_manager"]
    app.state.started_at = time.time()

    # Spawn the consumer as a background task owned by the current
    # event loop. The task holds its own reference so garbage collection
    # doesn't cancel it out from under us.
    consumer: RedisStreamConsumer = state["consumer"]
    consumer_task: asyncio.Task[None] = asyncio.create_task(
        consumer.run(), name="redis-stream-consumer"
    )
    app.state.consumer_task = consumer_task

    # Spawn the two WebSocket background tasks. Both honour the shared
    # stop_event so shutdown drains them in lockstep with the consumer.
    stats_task: asyncio.Task[None] = asyncio.create_task(
        stats_broadcast_loop(app, state["stop_event"]),
        name="ws-stats-broadcast",
    )
    app.state.stats_broadcast_task = stats_task

    heartbeat_task: asyncio.Task[None] = asyncio.create_task(
        state["ws_manager"].heartbeat_loop(
            float(settings.ws_heartbeat_interval_s), state["stop_event"]
        ),
        name="ws-heartbeat",
    )
    app.state.ws_heartbeat_task = heartbeat_task

    # Spawn the background segment merger. It owns its own cadence via
    # ``settings.background_merge_interval_s`` and honours the shared
    # stop_event so shutdown drains it alongside the consumer/WS tasks.
    merger_task: asyncio.Task[None] = asyncio.create_task(
        merge_loop(
            state["index"],
            state["stop_event"],
            float(settings.background_merge_interval_s),
        ),
        name="segment-merger",
    )
    app.state.merger_task = merger_task

    logger.info(
        "startup complete disk_dir=%s redis_url=%s redis_connected=%s",
        settings.disk_segment_dir,
        settings.redis_url,
        state["redis_connected"],
    )

    try:
        yield
    finally:
        # 1) Signal everything to wind down. ``stop`` is idempotent.
        state["stop_event"].set()
        try:
            await consumer.stop()
        except Exception as exc:  # noqa: BLE001 — shutdown best-effort
            logger.warning("consumer.stop raised on shutdown: %s", exc)

        # 2) Best-effort flush of ALL in-memory segments (current +
        #    flushed_memory FIFO) to disk so a restart doesn't drop the
        #    most-recent writes. ``flush_current`` alone would only
        #    rotate the active segment into the memory queue, leaving
        #    everything queued there unspilled. Failure here is not
        #    fatal — the next startup will rehydrate whatever did get
        #    spilled.
        try:
            await state["index"].flush_all_to_disk()
        except Exception as exc:  # noqa: BLE001
            logger.warning("index.flush_all_to_disk raised on shutdown: %s", exc)

        # 3) Wait for the consumer task to actually exit, bounded so a
        #    stuck XREADGROUP can't hang shutdown forever. If it misses
        #    the deadline we cancel and move on.
        if not consumer_task.done():
            try:
                await asyncio.wait_for(
                    consumer_task, timeout=_CONSUMER_SHUTDOWN_TIMEOUT_S
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "consumer task did not exit within %.1fs; cancelling",
                    _CONSUMER_SHUTDOWN_TIMEOUT_S,
                )
                consumer_task.cancel()
                try:
                    await consumer_task
                except (asyncio.CancelledError, Exception):
                    pass
            except asyncio.CancelledError:
                # Propagated from a wait_for cancellation — safe to
                # swallow on shutdown.
                pass
            except Exception as exc:  # noqa: BLE001
                logger.warning("consumer task exited with error: %s", exc)

        # 4) Drain the WebSocket + merger background tasks. All three
        #    loops check the shared stop_event so they unwind on their
        #    own; we just bound how long we'll wait. If any miss the
        #    deadline we cancel them — nothing they do at this point is
        #    critical (the merger's on-disk state is always durable).
        for task in (stats_task, heartbeat_task, merger_task):
            if task.done():
                continue
            try:
                await asyncio.wait_for(
                    task, timeout=_WS_TASK_SHUTDOWN_TIMEOUT_S
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "background task %s did not exit within %.1fs; cancelling",
                    task.get_name(),
                    _WS_TASK_SHUTDOWN_TIMEOUT_S,
                )
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "background task %s exited with error: %s",
                    task.get_name(),
                    exc,
                )

        # 5) Close every connected WS client cleanly. The manager's
        #    close_all swallows individual close errors so one bad
        #    socket can't stall shutdown.
        try:
            await state["ws_manager"].close_all()
        except Exception as exc:  # noqa: BLE001
            logger.warning("ws_manager.close_all raised on shutdown: %s", exc)

        # 6) Close the Redis client we constructed for the health probe.
        redis_client = state.get("redis_client")
        if redis_client is not None:
            try:
                await redis_client.aclose()
            except Exception as exc:  # noqa: BLE001
                logger.warning("redis aclose raised on shutdown: %s", exc)

        logger.info("shutdown complete")


# ---------------------------------------------------------------------------
# App factory + module-level app
# ---------------------------------------------------------------------------

# Project-root-relative template and static directories. ``Dockerfile``
# copies both directories to ``/app/`` so these paths resolve at
# runtime. Keeping them as module-level constants makes overriding in
# tests a one-liner.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATES_DIR = _PROJECT_ROOT / "templates"
_STATIC_DIR = _PROJECT_ROOT / "static"


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct a fresh FastAPI app, optionally with custom settings.

    ``build_app()`` with no argument behaves identically to the
    module-level ``app = build_app()``. Passing a :class:`Settings`
    instance lets tests point the app at an isolated tmp segment
    directory or a different Redis URL without mutating the global
    singleton.

    The ``settings`` object is stashed on ``app.state`` *before* the
    lifespan starts so :func:`lifespan` can read it. Mounting
    ``/static`` + wiring the router happens here too so a test that
    bypasses the lifespan still has a usable app.
    """
    s = settings if settings is not None else default_settings
    app = FastAPI(title="Real-Time Log Indexing", version="0.1.0", lifespan=lifespan)
    # Seed the settings on state before the lifespan starts so the
    # lifespan (and any early request) sees the right values.
    app.state.settings = s

    # Build the ConnectionManager here so the ``/ws`` route can be
    # registered at route-time (before the lifespan runs). The same
    # instance is stashed on ``app.state`` and reused by
    # ``build_app_state`` — otherwise we'd end up with two managers,
    # and the index callback would fire against a different client set
    # than the endpoint accepts.
    ws_manager = ConnectionManager()
    app.state.ws_manager = ws_manager

    app.include_router(router)
    register_ws_routes(app, ws_manager)

    # Mount static assets for the dashboard. ``StaticFiles`` resolves
    # paths relative to the working directory by default; using an
    # absolute path makes us robust to uvicorn being launched from
    # anywhere (docker CMD, pytest, a subprocess, …).
    app.mount(
        "/static",
        StaticFiles(directory=str(_STATIC_DIR)),
        name="static",
    )

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    # Keep the templates object on the app so tests / later commits
    # can reach it without re-constructing.
    app.state.templates = templates

    @app.get("/", response_class=HTMLResponse, name="dashboard")
    async def index_page(request: Request) -> HTMLResponse:
        """Serve the dashboard HTML.

        The template renders the service and level dropdowns from the
        canonical :mod:`src.sample_data` lists so adding a new service
        here flows into the UI without a separate client-side edit.
        """
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "services": SERVICES,
                "levels": LEVELS,
            },
        )

    return app


# Module-level app instance for ``uvicorn src.main:app``. The Dockerfile
# CMD and ``start.sh`` both import this symbol directly.
app = build_app()
