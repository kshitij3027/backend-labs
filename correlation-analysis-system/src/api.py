"""FastAPI application factory and HTTP surface for the Correlation Analysis System.

Endpoints: ``GET /health`` (C1), ``GET /api/v1/logs/recent`` (C3),
``GET /api/v1/correlations`` (C4), and the C7 completion of the v1 surface —
``GET /api/v1/correlations/stats``, ``GET /api/v1/correlations/types/{type}``
and ``GET /api/v1/dashboard``. The health payload's ``status``/``service``
values and the stats payload's exact 4-key shape are SPEC-VERBATIM contract
values — the unit tests and the C8 E2E verifier assert them exactly, so they
must never change (richer operational data belongs to /api/v1/dashboard only).

Every endpoint serves purely from in-memory accumulators — no handler performs
a Redis operation, so the whole read surface keeps answering mid-outage.

``/health`` always returns HTTP 200 while the process is alive: a degraded dependency
is signalled inside the body (``components``), never via a non-2xx status, so the
container healthcheck goes green the instant uvicorn binds.
"""

from __future__ import annotations

import time
from itertools import islice
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request

from src.models import CorrelationType

if TYPE_CHECKING:
    # Type-only import: src.main imports create_app from this module, so importing it
    # here at runtime would be a circular import.
    from src.main import Runtime

#: SPEC-VERBATIM /health contract values — never change these.
SERVICE_NAME = "correlation-analysis"
SERVICE_VERSION = "0.1.0"

#: Human-readable API title (not part of the /health contract).
API_TITLE = "Correlation Analysis System"

#: /api/v1/logs/recent clamps its ``count`` query param into [1, this] silently.
_RECENT_COUNT_MAX = 500

#: /api/v1/correlations[/types/{t}] clamp their ``limit`` into [1, this] silently.
_CORRELATIONS_LIMIT_MAX = 1000

#: /api/v1/dashboard feed sizes: the newest N of each in-memory accumulator.
_DASHBOARD_FEED_COUNT = 20
#: /api/v1/dashboard scatter cap (strength x confidence points).
_DASHBOARD_SCATTER_MAX = 200
#: /api/v1/dashboard timeline depth (10 s buckets — the engine's full history).
_DASHBOARD_TIMELINE_BUCKETS = 60

def _empty_stats() -> dict[str, Any]:
    """The SPEC-VERBATIM zeroed stats shape (the no-engine degradation value)."""
    return {"total": 0, "types": {}, "avg_strength": 0.0, "recent_count": 0}


def _memory_mb() -> float | None:
    """Resident memory (MiB) read from ``VmRSS`` in /proc/self/status.

    Linux-only by design (the containers are Linux). On platforms without procfs
    (e.g. a bare macOS run) this returns None and /health reports ``memory_mb: null``.
    """
    try:
        with open("/proc/self/status", encoding="ascii") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    # Line format: "VmRSS:      12345 kB"
                    return round(float(line.split()[1]) / 1024.0, 2)
    except (OSError, ValueError, IndexError):
        return None
    return None


def create_app(runtime: Runtime | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        runtime: Tests inject a pre-built :class:`src.main.Runtime` here, and the app
            then skips the lifespan entirely (no startup work, no background
            pipeline). When omitted (production: ``src.main.app``), the lifespan
            builds and attaches the Runtime on startup.
    """
    if runtime is not None:
        app = FastAPI(title=API_TITLE, version=SERVICE_VERSION)
        app.state.runtime = runtime
    else:
        # Deferred import (see the TYPE_CHECKING note above): safe here because by
        # the time create_app() is called, src.main has defined lifespan.
        from src.main import lifespan

        app = FastAPI(title=API_TITLE, version=SERVICE_VERSION, lifespan=lifespan)

    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        """Liveness probe — always HTTP 200 while the process is alive."""
        # Defensive: even if the runtime was never attached (misconfigured startup),
        # report healthy with degraded components rather than crashing the probe.
        rt = getattr(request.app.state, "runtime", None)
        uptime = 0.0 if rt is None else max(0.0, time.monotonic() - rt.started_at)
        store = None if rt is None else getattr(rt, "store", None)
        collector = None if rt is None else getattr(rt, "collector", None)
        return {
            "status": "healthy",  # SPEC-VERBATIM
            "service": SERVICE_NAME,  # SPEC-VERBATIM
            "version": SERVICE_VERSION,
            "uptime_seconds": uptime,
            "memory_mb": _memory_mb(),
            "components": {
                # None = no store wired at all; bool = last known Redis
                # availability (RedisStore re-probes lazily, at most every 5s).
                "redis": None if store is None else bool(store.available),
                "pipeline_running": False if rt is None else bool(rt.pipeline_running),
                "events_processed": 0 if collector is None else collector.events_total,
                "events_per_sec": (
                    0.0 if collector is None else round(float(collector.events_per_sec), 1)
                ),
                "parse_errors": 0 if collector is None else collector.parse_errors,
            },
        }

    @app.get("/api/v1/logs/recent")
    async def recent_logs(request: Request, count: int = 50) -> dict[str, Any]:
        """The newest parsed events, newest first.

        ``count`` is clamped silently into [1, 500] — an out-of-range value is a
        tuning mistake, not a client error, so it never yields a 422.
        """
        rt = getattr(request.app.state, "runtime", None)
        collector = None if rt is None else getattr(rt, "collector", None)
        if collector is None:
            # Defensive: no pipeline wired — an empty feed, never a 500.
            return {"events": []}
        clamped = max(1, min(count, _RECENT_COUNT_MAX))
        return {"events": [ev.model_dump() for ev in collector.recent(clamped)]}

    @app.get("/api/v1/correlations")
    async def correlations(
        request: Request, limit: int = 50, min_strength: float = 0.0
    ) -> dict[str, Any]:
        """The newest detected correlations, newest first.

        ``limit`` is clamped silently into [1, 1000] (same convention as
        /api/v1/logs/recent: out-of-range is a tuning mistake, not a 422);
        ``min_strength`` drops every correlation weaker than the given value.
        """
        rt = getattr(request.app.state, "runtime", None)
        engine = None if rt is None else getattr(rt, "engine", None)
        if engine is None:
            # Defensive: no engine wired — an empty feed, never a 500.
            return {"count": 0, "correlations": []}
        clamped = max(1, min(limit, _CORRELATIONS_LIMIT_MAX))
        found = engine.recent(limit=clamped, min_strength=min_strength)
        return {"count": len(found), "correlations": [corr.model_dump() for corr in found]}

    # NOTE: /stats is declared BEFORE /types/{...} on purpose — static segments
    # must never risk being captured by a later-declared path parameter.
    @app.get("/api/v1/correlations/stats")
    async def correlation_stats(request: Request) -> dict[str, Any]:
        """The SPEC-VERBATIM stats payload: EXACTLY total/types/avg_strength/recent_count.

        This response is a 4-key contract the E2E verifier asserts verbatim —
        never add fields here; the operational extras live in /api/v1/dashboard.
        """
        rt = getattr(request.app.state, "runtime", None)
        engine = None if rt is None else getattr(rt, "engine", None)
        if engine is None:
            # Defensive: no engine wired — the zeroed spec shape, never a 500.
            return _empty_stats()
        return engine.stats()

    @app.get("/api/v1/correlations/types/{correlation_type}")
    async def correlations_by_type(
        request: Request, correlation_type: CorrelationType, limit: int = 100
    ) -> dict[str, Any]:
        """The newest correlations of one type, newest first.

        The path parameter is typed as the CorrelationType enum, so an unknown
        type is FastAPI's automatic 422 — unlike the numeric params, a bogus
        type is a wrong API call, not a tuning mistake. ``limit`` clamps
        silently into [1, 1000] as everywhere else.
        """
        rt = getattr(request.app.state, "runtime", None)
        engine = None if rt is None else getattr(rt, "engine", None)
        if engine is None:
            # Defensive: no engine wired — an empty feed, never a 500.
            return {"correlation_type": correlation_type.value, "count": 0, "correlations": []}
        clamped = max(1, min(limit, _CORRELATIONS_LIMIT_MAX))
        found = engine.recent(limit=clamped, ctype=correlation_type)
        return {
            "correlation_type": correlation_type.value,
            "count": len(found),
            "correlations": [corr.model_dump() for corr in found],
        }

    @app.get("/api/v1/dashboard")
    async def dashboard(request: Request) -> dict[str, Any]:
        """Everything the React dashboard needs in ONE 5-second poll.

        Every section is served from in-memory accumulators (engine deque and
        counters, collector buffer, alert history); the request path performs
        ZERO Redis operations — even the ``status.redis`` flag is the store's
        last observed health, not a probe — so the dashboard stays live through
        a Redis outage. Each section degrades to an empty shape when its
        runtime piece is absent.
        """
        now = time.time()
        rt = getattr(request.app.state, "runtime", None)
        store = None if rt is None else getattr(rt, "store", None)
        collector = None if rt is None else getattr(rt, "collector", None)
        engine = None if rt is None else getattr(rt, "engine", None)
        alerts = None if rt is None else getattr(rt, "alerts", None)
        generator = None if rt is None else getattr(rt, "generator", None)

        active_scenario: str | None = None
        if generator is not None:
            try:
                active = generator.active_scenario(now)
            except AttributeError:  # duck-typed generator without a scenario clock
                active = None
            if active is not None:
                # ScenarioKind is a str-Enum; fall back to str() for stubs.
                active_scenario = getattr(active, "value", str(active))

        if engine is not None:
            engine_stats = engine.stats(now)
            timeline = engine.timeline(_DASHBOARD_TIMELINE_BUCKETS)
            # Newest N points, O(N) off the right end of the deque.
            scatter = [
                {
                    "strength": corr.strength,
                    "confidence": corr.confidence,
                    "type": corr.correlation_type.value,
                    "detected_at": corr.detected_at,
                }
                for corr in islice(reversed(engine.correlations), _DASHBOARD_SCATTER_MAX)
            ]
            matrix = engine.matrix()
            recent_correlations = [
                corr.model_dump() for corr in engine.recent(_DASHBOARD_FEED_COUNT)
            ]
        else:  # defensive: no engine wired — empty shapes, never a 500
            engine_stats = _empty_stats()
            timeline, scatter, recent_correlations = [], [], []
            matrix = {"sources": [], "cells": []}

        return {
            "generated_at": now,
            "status": {
                "healthy": True,  # same liveness semantics as /health
                "redis": False if store is None else bool(store.healthy),
                "pipeline_running": False if rt is None else bool(rt.pipeline_running),
                "active_scenario": active_scenario,
            },
            "stats": {
                # The 4 spec keys first, then the operational extras that the
                # pure /api/v1/correlations/stats contract must never carry.
                **engine_stats,
                "events_processed": 0 if collector is None else collector.events_total,
                "events_per_sec": (
                    0.0 if collector is None else round(float(collector.events_per_sec), 1)
                ),
                "parse_errors": 0 if collector is None else collector.parse_errors,
                "uptime_seconds": (
                    0.0 if rt is None else max(0.0, time.monotonic() - rt.started_at)
                ),
                "memory_mb": _memory_mb(),
                "alerts_total": 0 if alerts is None else getattr(alerts, "total", 0),
            },
            "timeline": timeline,
            "scatter": scatter,
            "matrix": matrix,
            "recent_correlations": recent_correlations,
            "recent_logs": (
                []
                if collector is None
                else [ev.model_dump() for ev in collector.recent(_DASHBOARD_FEED_COUNT)]
            ),
            "alerts": (
                []
                if alerts is None
                else [alert.model_dump() for alert in alerts.recent(_DASHBOARD_FEED_COUNT)]
            ),
        }

    return app
