"""Application entrypoint and runtime wiring for the NLP Log Processing Engine.

Defines :class:`Runtime` — the single container for per-process state — and the FastAPI
``lifespan`` that builds it on startup and tears it down on shutdown. The module-level
``app`` is what uvicorn serves (``python -m uvicorn src.main:app``).

C1 wired only the settings. C7 adds the NLP engine: :meth:`Runtime.build_loaded` constructs
an :class:`~src.nlp.NLPEngine` and ``load()``s it (spaCy + the intent model + VADER + YAKE,
once), and the production ``lifespan`` uses it so the served app comes up with all models
ready (``GET /api/health`` then reports ``analyzer_ready=true`` for real). :meth:`Runtime.build`
still leaves ``engine`` ``None``, so injecting a pre-built Runtime via
``create_app(runtime=Runtime.build(...))`` (the cheap unit-test path) skips the lifespan and
loads no models — the HTTP surface stays hermetic.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi import FastAPI

from src.api import create_app
from src.config import Settings, get_settings
from src.stats import StatsAggregator

if TYPE_CHECKING:
    # Type-only: importing the engine at runtime would pull spaCy/sklearn/VADER/YAKE into
    # every `import src.main` (e.g. the cheap unit-test path). The real import is deferred to
    # build_loaded / the lifespan, which are the only places that actually load models.
    from src.nlp import NLPEngine

logger = logging.getLogger(__name__)


@dataclass
class Runtime:
    """Per-process runtime state shared by the API handlers.

    Holds the settings and, once loaded, the :class:`~src.nlp.NLPEngine`. ``engine`` is
    ``None`` for the cheap injected-runtime test path (:meth:`build`) and a fully-loaded
    engine for the production app (:meth:`build_loaded`). ``GET /api/health`` and the analyze
    routes read it defensively via ``getattr(runtime, "engine", None)``.

    ``stats`` is the process-local :class:`~src.stats.StatsAggregator` powering ``GET
    /api/stats``. It is cheap in-memory state (no models, no I/O), so **both** build paths
    create one — even the not-loaded unit-test runtime has working, if empty, stats. The
    analyze routes fold each result into it and ``/api/stats`` reads its snapshot, both via a
    defensive ``getattr(runtime, "stats", None)``.
    """

    settings: Settings
    engine: NLPEngine | None = None
    stats: StatsAggregator | None = None

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a Runtime **without** loading any models (``engine=None``).

        The cheap path: does no I/O and builds no engine, so the injected-runtime unit tests
        stay hermetic and ``GET /api/health`` degrades to ``analyzer_ready=true`` (no engine
        wired). A :class:`~src.stats.StatsAggregator` is still attached — it is pure in-memory
        state, so ``/api/stats`` works (empty) even on this cheap path.
        """
        return cls(settings=settings, stats=cls._build_stats(settings))

    @classmethod
    def build_loaded(cls, settings: Settings) -> Runtime:
        """Construct a Runtime with a fully **loaded** :class:`~src.nlp.NLPEngine` attached.

        The heavy path used by the production ``lifespan`` (and by the integration tests):
        builds the engine and ``load()``s it once — spaCy, the intent pipeline (baked
        artifact or freshly trained), VADER and YAKE — so ``engine.ready`` is ``True`` and the
        analyze routes serve real results. The :class:`~src.nlp.NLPEngine` import is deferred
        to here to keep ``import src.main`` free of the heavy NLP stack.
        """
        from src.nlp import NLPEngine

        engine = NLPEngine().load()
        return cls(settings=settings, engine=engine, stats=cls._build_stats(settings))

    @staticmethod
    def _build_stats(settings: Settings) -> StatsAggregator:
        """Construct the rolling :class:`~src.stats.StatsAggregator` from settings.

        Shared by both build paths so the window / trending-top-k tunables are read from
        :class:`~src.config.Settings` in exactly one place.
        """
        return StatsAggregator(
            window=settings.stats_window,
            trending_top_k=settings.trending_top_k,
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build a **loaded** Runtime on startup, attach it to ``app.state``, tear it down on exit.

    Production entry point: :meth:`Runtime.build_loaded` loads all NLP models before the app
    starts serving (blocking — the health ``start_period`` gives it ample margin), so the live
    app reports true readiness. Tests never enter this path: they inject a pre-built Runtime
    via ``create_app(runtime=...)``, skipping the lifespan (and the model load) entirely.
    """
    settings = get_settings()
    runtime = Runtime.build_loaded(settings)
    app.state.runtime = runtime
    logger.info(
        "runtime initialised (log_level=%s, analyzer_ready=%s)",
        settings.log_level,
        getattr(runtime.engine, "ready", False),
    )

    try:
        yield
    finally:
        # The engine is pure in-memory state (loaded models) with no handles/sockets to
        # release. Later commits cancel their background live-stream task here.
        pass


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so the
#: lifespan above constructs one on startup.
app = create_app()
