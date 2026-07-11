"""Application entrypoint and runtime wiring for the RCA Analysis Engine.

Defines :class:`Runtime` — the single container for per-process state (settings
plus the analyzer, the bounded in-memory incident history, the WebSocket connection
manager and the background live-stream task) — and the FastAPI ``lifespan`` that
builds it on startup and tears it down on shutdown. The module-level ``app`` is what
uvicorn serves (``python -m uvicorn src.main:app``).

C1 wires only the settings and empty placeholders; later commits populate
``analyzer`` (C2+), ``connection_manager`` (C6) and ``live_task`` (C8, gated by
``settings.live_stream_enabled``).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI

from src.api import create_app
from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    """Per-process runtime state shared by the API handlers and the live loop."""

    settings: Settings
    #: The RCA analyzer (owns causal analysis + report assembly); wired from C2 on.
    analyzer: Any | None = None
    #: Bounded in-memory incident history (newest appended last); filled from C5.
    incident_history: list[Any] = field(default_factory=list)
    #: WebSocket connection manager for real-time push; wired in C6.
    connection_manager: Any | None = None
    #: Background live-stream task; started by the lifespan in C8 when
    #: settings.live_stream_enabled is true (always None under tests, which inject a
    #: pre-built Runtime and never enter the lifespan).
    live_task: asyncio.Task | None = None

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a fresh Runtime holding the settings and the RCA analyzer.

        The analyzer is built here (C2) so both the injected-runtime test path and
        the production lifespan share one construction site. Nothing here touches the
        network or spawns a task: the connection manager is attached in C6 and the
        live-stream task is started by the lifespan only when enabled (C8). The
        import is deferred to keep module import order simple and avoid any import
        cycle through the analysis package.
        """
        from src.analysis import RCAAnalyzer

        return cls(settings=settings, analyzer=RCAAnalyzer(settings))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the Runtime on startup, attach it to app.state, tear it down on exit.

    Tests never enter this path — they inject a pre-built Runtime via
    ``create_app(runtime=...)`` — so nothing here runs under pytest. The background
    live-stream loop is added in C8 (guarded by ``settings.live_stream_enabled``,
    which defaults off); the teardown below already cancels ``live_task`` defensively
    so it stays correct once that loop lands.
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    app.state.runtime = runtime

    try:
        yield
    finally:
        task = runtime.live_task
        runtime.live_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: the lifespan above constructs one on startup.
app = create_app()
