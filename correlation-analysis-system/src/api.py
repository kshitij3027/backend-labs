"""FastAPI application factory and HTTP surface for the Correlation Analysis System.

Endpoints so far: ``GET /health`` (C1) and ``GET /api/v1/logs/recent`` (C3). The
health payload's ``status`` and ``service`` values are SPEC-VERBATIM contract
values — the unit tests and the C8 E2E verifier assert them exactly, so they
must never change.

``/health`` always returns HTTP 200 while the process is alive: a degraded dependency
is signalled inside the body (``components``), never via a non-2xx status, so the
container healthcheck goes green the instant uvicorn binds.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request

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

    return app
