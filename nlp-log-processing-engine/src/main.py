"""Application entrypoint and runtime wiring for the NLP Log Processing Engine.

Defines :class:`Runtime` — the single container for per-process state — and the FastAPI
``lifespan`` that builds it on startup and tears it down on shutdown. The module-level
``app`` is what uvicorn serves (``python -m uvicorn src.main:app``).

C1 wires only the settings. The NLP engine (which loads spaCy + the intent model + VADER
+ YAKE once and exposes a ``.ready`` flag) is constructed in :meth:`Runtime.build` and
loaded by the lifespan in a later commit. Injecting a pre-built Runtime via
``create_app(runtime=...)`` (the test path) skips the lifespan entirely, so tests are
hermetic.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI

from src.api import create_app
from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    """Per-process runtime state shared by the API handlers.

    C1 holds only the settings. Later commits add an ``engine`` attribute (the loaded
    ``NLPEngine``) here; ``GET /api/health`` already reads it defensively via
    ``getattr(runtime, "engine", None)``, so that addition needs no restructuring.
    """

    settings: Settings

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a fresh Runtime.

        A single construction site shared by the injected-runtime test path and the
        production lifespan. C1 does no I/O and spawns nothing; later commits build and
        attach the NLP engine here so both paths share one wiring point.
        """
        return cls(settings=settings)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the Runtime on startup, attach it to ``app.state``, tear it down on exit.

    Tests never enter this path when they inject a pre-built Runtime via
    ``create_app(runtime=...)``. C1 has nothing to start (no engine load, no background
    loop) and nothing to release; the structured teardown below is the drop-in point for
    later commits (engine handles, a live-stream task) and keeps their addition
    restructure-free.
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    app.state.runtime = runtime
    logger.info("runtime initialised (log_level=%s)", settings.log_level)

    try:
        yield
    finally:
        # Nothing to tear down yet — no engine handles to release and no live-stream loop
        # wired in C1. Later commits cancel their background task here.
        pass


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so the
#: lifespan above constructs one on startup.
app = create_app()
