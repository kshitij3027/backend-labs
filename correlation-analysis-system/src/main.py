"""Application entrypoint and runtime wiring for the Correlation Analysis System.

Defines :class:`Runtime` — the single container for per-process state (settings,
the Redis store, pipeline bookkeeping) — and the FastAPI ``lifespan`` that builds it
on startup. The module-level ``app`` is what uvicorn serves
(``python -m uvicorn src.main:app``).
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI

from src.api import create_app
from src.config import Settings, get_settings


@dataclass
class Runtime:
    """Per-process runtime state shared by the API handlers and the pipeline.

    C1 carries only settings + uptime bookkeeping; the Redis store and the background
    pipeline task attach here in C3.
    """

    settings: Settings
    #: time.monotonic() at build time — /health derives uptime_seconds from this
    #: (monotonic, so wall-clock adjustments can never yield negative uptime).
    started_at: float
    #: The RedisStore once C3 lands; None means "no store wired" (/health reports null).
    store: Any = None
    #: True while the background pipeline task is running (flipped in C3).
    pipeline_running: bool = False

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a fresh Runtime for the given settings."""
        return cls(settings=settings, started_at=time.monotonic())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the Runtime on startup and attach it to ``app.state``.

    Tests never enter this path — they inject a pre-built Runtime via
    ``create_app(runtime=...)`` instead, so nothing here runs under pytest.
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    app.state.runtime = runtime

    if settings.pipeline_enabled:
        # C3 hook: the background pipeline task (1s generate->parse->buffer->aggregate
        # tick + 2s detection cycle) starts HERE via asyncio.create_task(...), flipping
        # runtime.pipeline_running = True. Nothing to start until the collector, store
        # and engine land in C3.
        pass

    try:
        yield
    finally:
        # Teardown mirror of the C3 hook: the pipeline task is cancelled/awaited and
        # the store closed here once they exist.
        runtime.pipeline_running = False


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: the lifespan above constructs one on startup.
app = create_app()
