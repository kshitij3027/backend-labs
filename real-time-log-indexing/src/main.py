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
from src.config import Settings, settings as default_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.logging_setup import setup_logging
from src.stream.redis_consumer import RedisStreamConsumer


logger = logging.getLogger(__name__)


# Maximum time we'll wait for the background consumer task to unwind
# during shutdown before cancelling it outright. Kept short so `make
# down` / pytest teardown doesn't stall on a stuck XREADGROUP.
_CONSUMER_SHUTDOWN_TIMEOUT_S: float = 5.0


# ---------------------------------------------------------------------------
# App state construction
# ---------------------------------------------------------------------------

async def build_app_state(settings: Settings) -> dict:
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

    Splitting this out keeps the lifespan readable (it just plumbs
    the dict into ``app.state``, spawns the consumer task, and yields)
    and makes the construction order trivial to unit-test.
    """
    tokenizer = LogTokenizer()
    disk_dir = Path(settings.disk_segment_dir)

    index = InvertedIndex(settings, tokenizer, disk_dir=disk_dir)
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
    }


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

    state = await build_app_state(settings)
    app.state.index = state["index"]
    app.state.consumer = state["consumer"]
    app.state.redis_client = state["redis_client"]
    app.state.redis_connected = state["redis_connected"]
    app.state.stop_event = state["stop_event"]
    app.state.settings = settings
    app.state.started_at = time.time()

    # Spawn the consumer as a background task owned by the current
    # event loop. The task holds its own reference so garbage collection
    # doesn't cancel it out from under us.
    consumer: RedisStreamConsumer = state["consumer"]
    consumer_task: asyncio.Task[None] = asyncio.create_task(
        consumer.run(), name="redis-stream-consumer"
    )
    app.state.consumer_task = consumer_task
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

        # 2) Best-effort flush of the current in-memory segment so a
        #    restart doesn't drop the most-recent writes. Failure here
        #    is not fatal — the next startup will simply rehydrate
        #    whatever did get spilled.
        try:
            await state["index"].flush_current()
        except Exception as exc:  # noqa: BLE001
            logger.warning("index.flush_current raised on shutdown: %s", exc)

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

        # 4) Close the Redis client we constructed for the health probe.
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

    app.include_router(router)

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

        In Commit 7 this is a placeholder — the real dashboard (with
        search, filters, live stats card, and the WebSocket hookup)
        lands in Commit 10 behind this same URL.
        """
        return templates.TemplateResponse(
            "dashboard.html", {"request": request}
        )

    return app


# Module-level app instance for ``uvicorn src.main:app``. The Dockerfile
# CMD and ``start.sh`` both import this symbol directly.
app = build_app()
