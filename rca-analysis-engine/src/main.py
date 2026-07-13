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
import time
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
    #: Latest live-stream incident's injected ground truth (C10) —
    #: ``{"incident_id", "root_cause_event_id", "root_cause_service"}`` — tagged each tick
    #: by the live loop so ``GET /api/debug/ground-truth`` can surface it. ``None`` until
    #: the loop runs (it is off by default), which the endpoint maps to ``{}``.
    last_ground_truth: dict | None = None

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a fresh Runtime holding the settings and the RCA analyzer.

        The analyzer is built here (C2) so both the injected-runtime test path and
        the production lifespan share one construction site. Nothing here touches the
        network or spawns a task: the connection manager is constructed here (C6 — an
        empty in-memory registry, no I/O) and the live-stream task is started by the
        lifespan only when enabled (C8). The imports are deferred to keep module
        import order simple and avoid any import cycle through the analysis package.
        """
        from src.analysis import RCAAnalyzer
        from src.ws import ConnectionManager

        return cls(
            settings=settings,
            analyzer=RCAAnalyzer(settings),
            connection_manager=ConnectionManager(),
        )


async def _live_stream_loop(runtime: Runtime) -> None:
    """Background loop: periodically analyze a synthetic incident and broadcast it (C8).

    Each tick generates a small synthetic incident (seed ``live_stream_seed + counter`` so
    successive incidents vary yet stay reproducible), feeds it through a persistent
    :class:`~src.analysis.incremental.IncrementalAnalyzer` (whose warm-started re-rank reuses
    the previous tick's PageRank), records the snapshot in the shared bounded history, and
    broadcasts it to every connected ``/ws`` client as
    ``{"type": "incident_update", "data": <report>}``.

    Resilience is deliberate: every tick is wrapped so a single bad incident is logged and
    skipped rather than killing the stream. The guard is ``except Exception`` (never a bare
    ``except``), and :class:`asyncio.CancelledError` — not an ``Exception`` subclass — is
    re-raised so the loop exits promptly and cleanly when the lifespan cancels it. The
    cadence is self-correcting: it sleeps ``max(0.1, interval - elapsed)`` so slow ticks
    don't drift and a misconfigured tiny interval can't become a busy-spin.
    """
    # Deferred imports keep module import order simple (no import-time pull of the heavy
    # analysis package) and let tests monkeypatch ``src.generators.generate_incident``.
    from src import generators
    from src.analysis.incremental import IncrementalAnalyzer

    settings = runtime.settings
    manager = runtime.connection_manager
    analyzer = runtime.analyzer
    incremental = IncrementalAnalyzer(settings)
    counter = 0

    while True:
        started = time.monotonic()
        try:
            scenario = generators.generate_incident(seed=settings.live_stream_seed + counter)
            incremental.add_events(scenario.events)
            report = incremental.snapshot()
            if analyzer is not None:
                analyzer.remember(report)
            # C10: tag this tick's injected ground truth so GET /api/debug/ground-truth can
            # surface the known root cause of the most recent live incident. Harmless when
            # nothing consumes it; the e2e verifier's primary ground truth is in-process.
            runtime.last_ground_truth = {
                "incident_id": report.incident_id,
                "root_cause_event_id": scenario.root_cause_event_id,
                "root_cause_service": scenario.root_cause_service,
            }
            if manager is not None:
                await manager.broadcast(
                    {"type": "incident_update", "data": report.model_dump(mode="json")}
                )
        except asyncio.CancelledError:
            raise  # cancellation must propagate so the loop exits cleanly
        except Exception:  # noqa: BLE001 - one bad tick must never kill the stream
            logger.exception("live-stream tick %d failed; continuing", counter)

        counter += 1
        elapsed = time.monotonic() - started
        await asyncio.sleep(max(0.1, settings.live_stream_interval - elapsed))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the Runtime on startup, attach it to app.state, tear it down on exit.

    Tests never enter this path when they inject a pre-built Runtime via
    ``create_app(runtime=...)`` — so nothing here runs under those fixtures. The background
    live-stream loop (C8) is started only when ``settings.live_stream_enabled`` is true
    (defaults off, so CI and the injected-runtime path never spin it up); the teardown
    cancels ``live_task`` and awaits its cancellation under ``suppress(CancelledError)``.
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    app.state.runtime = runtime

    try:
        if settings.live_stream_enabled:
            runtime.live_task = asyncio.create_task(_live_stream_loop(runtime))
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
