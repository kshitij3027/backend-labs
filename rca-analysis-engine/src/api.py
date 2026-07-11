"""FastAPI application factory and HTTP surface for the RCA Analysis Engine.

Endpoints (C1): ``GET /api/health`` — the spec-verbatim liveness probe. Later
commits extend this same factory with ``POST /api/analyze-incident`` and
``GET /api/incidents[/{id}[/report]]`` (C5), the CORS middleware + ``/ws``
WebSocket (C6), and ``GET /api/calibration`` (C9). Handlers read shared state off
``request.app.state.runtime`` (attached by the lifespan, or injected by tests).

``/api/health`` always returns HTTP 200 while the process is alive and is fully
dependency-free: the analyzer is in-memory and ready the instant uvicorn binds, so
the body is the exact constant spec contract
``{"status": "healthy", "analyzer_ready": true}``. The unit tests and the C10 E2E
verifier assert it verbatim, so it must never change — the two keys, nothing more.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import FastAPI

if TYPE_CHECKING:
    # Type-only import: src.main imports create_app from this module, so importing
    # Runtime here at runtime would be a circular import.
    from src.main import Runtime

#: Human-readable API title / version (shown in the OpenAPI docs; not a contract).
API_TITLE = "RCA Analysis Engine"
API_VERSION = "0.1.0"

#: SPEC-VERBATIM /api/health body — never change these keys/values.
_HEALTH_BODY: dict[str, Any] = {"status": "healthy", "analyzer_ready": True}


def create_app(runtime: Runtime | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        runtime: Tests inject a pre-built :class:`src.main.Runtime` here, and the
            app then skips the FastAPI lifespan entirely (no startup work, no
            background live-stream loop). When omitted (production:
            ``src.main.app``), the lifespan builds and attaches the Runtime on
            startup.
    """
    if runtime is not None:
        app = FastAPI(title=API_TITLE, version=API_VERSION)
        app.state.runtime = runtime
    else:
        # Deferred import (see the TYPE_CHECKING note above): safe here because by
        # the time create_app() is called, src.main has finished defining lifespan.
        from src.main import lifespan

        app = FastAPI(title=API_TITLE, version=API_VERSION, lifespan=lifespan)

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        """Liveness probe — dependency-free, always HTTP 200 while the process is alive.

        The body is a constant spec contract (never derived from runtime state), so
        the probe cannot fail while the process is serving requests. A fresh dict is
        returned each call so the module-level constant can never be mutated.
        """
        return dict(_HEALTH_BODY)

    return app
