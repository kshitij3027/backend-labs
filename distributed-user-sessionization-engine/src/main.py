"""FastAPI application entry point for the Distributed User Sessionization Engine."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from src.api.analytics import router as analytics_router
from src.api.events import router as events_router
from src.api.sessions import router as sessions_router
from src.config import get_config
from src.redis_store import RedisStore
from src.session_engine import SessionEngine

logger = logging.getLogger(__name__)


async def _cleanup_loop(
    engine: SessionEngine, interval: float, stop_event: asyncio.Event
) -> None:
    """Periodically run cleanup_idle_sessions until stop_event is set."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        if not stop_event.is_set():
            await engine.cleanup_idle_sessions()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = get_config()
    app.state.config = config

    # Initialize Redis store
    redis_store = RedisStore(config)
    await redis_store.connect()
    app.state.redis_store = redis_store

    # Initialize engine with Redis backing
    engine = SessionEngine(config, redis_store=redis_store)
    app.state.session_engine = engine

    # Start partition workers
    await engine.start_workers()

    # Start cleanup background task
    stop_event = asyncio.Event()
    cleanup_task = asyncio.create_task(
        _cleanup_loop(engine, config.cleanup_interval_seconds, stop_event),
        name="cleanup-loop",
    )

    logger.info("Sessionization engine started (port=%s)", config.port)
    yield

    # Graceful shutdown
    logger.info("Sessionization engine shutting down")
    stop_event.set()
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    await engine.stop_workers()
    await engine.flush_to_redis()
    await redis_store.close()


app = FastAPI(title="Distributed User Sessionization Engine", lifespan=lifespan)

app.include_router(events_router)
app.include_router(sessions_router)
app.include_router(analytics_router)


@app.get("/health")
async def health():
    return JSONResponse({"status": "healthy"})


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content="<h1>Distributed User Sessionization Engine</h1><p>Dashboard coming soon.</p>")
