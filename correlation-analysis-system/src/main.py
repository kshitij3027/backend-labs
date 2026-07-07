"""Application entrypoint and runtime wiring for the Correlation Analysis System.

Defines :class:`Runtime` — the single container for per-process state (settings,
the generator -> collector -> aggregator -> engine pipeline stages, the Redis
store, and the background pipeline task) — plus the FastAPI ``lifespan`` that
builds it on startup and starts/stops the pipeline. The module-level ``app`` is
what uvicorn serves (``python -m uvicorn src.main:app``).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

from fastapi import FastAPI

from src.aggregation import MetricAggregator
from src.alerts import AlertManager
from src.api import create_app
from src.collector import LogCollector
from src.config import Settings, get_settings
from src.engine import CorrelationEngine
from src.generators import LogGenerator
from src.models import LogEvent
from src.patterns import PatternLearner
from src.store import RedisStore

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    """Per-process runtime state shared by the API handlers and the pipeline."""

    settings: Settings
    #: time.monotonic() at build time — /health derives uptime_seconds from this
    #: (monotonic, so wall-clock adjustments can never yield negative uptime).
    started_at: float
    store: RedisStore | None = None
    generator: LogGenerator | None = None
    aggregator: MetricAggregator | None = None
    collector: LogCollector | None = None
    #: The correlation engine (detection runs every detection_interval_seconds).
    engine: CorrelationEngine | None = None
    #: The pattern learner the engine assesses/records against each detection
    #: cycle; kept on the runtime so operators/tests can inspect its baselines.
    patterns: PatternLearner | None = None
    #: The alert manager the engine consults each detection cycle; kept on the
    #: runtime so the C7 dashboard endpoint can read its recent() feed directly.
    alerts: AlertManager | None = None
    #: The background pipeline task; None until the lifespan starts it (and always
    #: None under tests, which inject a pre-built Runtime and drive ticks manually).
    pipeline_task: asyncio.Task | None = None

    @property
    def pipeline_running(self) -> bool:
        """True while the background pipeline task exists and has not finished."""
        return self.pipeline_task is not None and not self.pipeline_task.done()

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a fresh Runtime with the full collection pipeline wired.

        Nothing here touches the network: the RedisStore connects lazily on its
        first operation (and degrades gracefully if Redis never answers).
        """
        store = RedisStore(settings.redis_url)
        generator = LogGenerator(settings)
        aggregator = MetricAggregator()
        collector = LogCollector(settings, generator, aggregator, store)
        alerts = AlertManager(settings)
        patterns = PatternLearner(settings, store)
        engine = CorrelationEngine(
            settings, aggregator, store, patterns=patterns, alerts=alerts
        )
        return cls(
            settings=settings,
            started_at=time.monotonic(),
            store=store,
            generator=generator,
            aggregator=aggregator,
            collector=collector,
            engine=engine,
            patterns=patterns,
            alerts=alerts,
        )


async def _pipeline_loop(runtime: Runtime) -> None:
    """The 1-second generate->parse->buffer->aggregate heartbeat (runs until
    cancelled), with a synchronous detection pass every detection interval.

    Each iteration is individually guarded: one bad tick logs and continues,
    because the pipeline must never die from a transient failure. Cancellation
    still works — CancelledError is a BaseException, so ``except Exception``
    below cannot swallow it and shutdown propagates cleanly out of the loop.
    """
    settings = runtime.settings
    collector = runtime.collector
    engine = runtime.engine
    if collector is None:  # defensive: Runtime.build always wires one
        logger.error("pipeline started without a collector; nothing to run")
        return

    interval = settings.generation_interval_seconds
    last_detection = 0.0
    # Events parsed since the last detection pass — handed to the engine as its
    # new_events batch, then reset (so nothing is re-detected as "new").
    pending_new: list[LogEvent] = []
    while True:
        t0 = time.perf_counter()
        try:
            now = time.time()
            pending_new.extend(collector.tick(now))
            if now - last_detection >= settings.detection_interval_seconds:
                last_detection = now
                batch, pending_new = pending_new, []
                if engine is not None:
                    cutoff = now - settings.window_seconds
                    window_events = [ev for ev in collector.buffer if ev.timestamp >= cutoff]
                    # Synchronous detection + its single pipelined Redis flush.
                    engine.detect(batch, window_events, now)
        except Exception:  # noqa: BLE001 — a bad tick must not kill the pipeline
            logger.exception("pipeline tick failed; continuing")
        elapsed = time.perf_counter() - t0
        await asyncio.sleep(max(0.05, interval - elapsed))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build the Runtime on startup, start the pipeline task, tear both down.

    Tests never enter this path — they inject a pre-built Runtime via
    ``create_app(runtime=...)`` instead, so nothing here runs under pytest (and
    the compose `test` service additionally sets PIPELINE_ENABLED=false).
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    app.state.runtime = runtime

    if settings.pipeline_enabled:
        runtime.pipeline_task = asyncio.create_task(_pipeline_loop(runtime))
        logger.info(
            "pipeline task started (tick=%.2fs, detection=%.2fs)",
            settings.generation_interval_seconds,
            settings.detection_interval_seconds,
        )

    try:
        yield
    finally:
        task = runtime.pipeline_task
        runtime.pipeline_task = None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: the lifespan above constructs one on startup.
app = create_app()
