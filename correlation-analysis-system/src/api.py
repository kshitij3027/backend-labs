"""FastAPI application factory and HTTP surface for the Correlation Analysis System.

C1 exposes only ``GET /health``. The health payload's ``status`` and ``service``
values are SPEC-VERBATIM contract values — the unit tests and the C8 E2E verifier
assert them exactly, so they must never change.

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
        pipeline_running = False if rt is None else bool(rt.pipeline_running)
        return {
            "status": "healthy",  # SPEC-VERBATIM
            "service": SERVICE_NAME,  # SPEC-VERBATIM
            "version": SERVICE_VERSION,
            "uptime_seconds": uptime,
            "memory_mb": _memory_mb(),
            "components": {
                # Null until the Redis store lands in C3 (a real store.ping() is
                # wired then); null distinguishes "not wired" from a failed ping.
                "redis": None,
                "pipeline_running": pipeline_running,
            },
        }

    return app
