"""FastAPI application factory and HTTP surface for the NLP Log Processing Engine.

``create_app(runtime=None)`` is the single construction site for the app:

* **Production** (``src.main.app = create_app()``) builds ``FastAPI(lifespan=lifespan)``;
  the lifespan constructs the :class:`~src.main.Runtime` (settings now, the loaded NLP
  engine later) and attaches it to ``app.state.runtime`` on startup.
* **Tests** call ``create_app(runtime=Runtime.build(...))``. A supplied runtime is
  attached directly to ``app.state.runtime`` and the lifespan is skipped entirely — no
  startup work, no model loading, no background loop — so the HTTP surface is exercised
  hermetically.

All routes are declared **inline** in this factory (no ``APIRouter``) so each closes over
the app it belongs to; later commits add ``/api/analyze`` and friends right here. Handlers
read shared state defensively off ``request.app.state.runtime`` using
``getattr(..., default)`` and degrade gracefully rather than raising: a missing or
half-wired runtime yields a safe fallback, never a 500.

``/api/health`` is a FROZEN contract — exactly ``{"status": "healthy",
"analyzer_ready": <bool>}`` (the two keys, nothing more). It is dependency-free and always
returns HTTP 200 while the process is alive. ``analyzer_ready`` is computed from
``runtime.engine.ready`` when an engine is wired (a later commit) and defaults to ``True``
before then. The unit tests and the E2E verifier assert this body, so its shape must never
change.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request

if TYPE_CHECKING:
    # Type-only import: src.main imports create_app from this module, so importing Runtime
    # at runtime here would be a circular import.
    from src.main import Runtime

logger = logging.getLogger(__name__)

#: Human-readable API title / version (shown in the OpenAPI docs; not a contract).
API_TITLE = "NLP Log Processing Engine"
API_VERSION = "0.1.0"


def create_app(runtime: Runtime | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        runtime: Tests inject a pre-built :class:`src.main.Runtime` here, and the app then
            skips the FastAPI lifespan entirely (no startup work, no model load). When
            omitted (production: ``src.main.app``), the lifespan builds and attaches the
            Runtime on startup.
    """
    if runtime is not None:
        app = FastAPI(title=API_TITLE, version=API_VERSION)
        app.state.runtime = runtime
    else:
        # Deferred import (see the TYPE_CHECKING note above): safe here because by the time
        # create_app() is called, src.main has finished defining lifespan.
        from src.main import lifespan

        app = FastAPI(title=API_TITLE, version=API_VERSION, lifespan=lifespan)

    @app.get("/api/health")
    async def health(request: Request) -> dict[str, Any]:
        """Liveness probe — dependency-free, always HTTP 200 while the process is alive.

        Returns the frozen contract ``{"status": "healthy", "analyzer_ready": <bool>}``.
        ``analyzer_ready`` is read defensively: ``True`` unless an NLP engine is wired on
        the runtime and reports itself not-ready. Nothing here raises — a missing runtime
        or engine simply degrades to ``True`` (the process is up), so the probe never fails
        while uvicorn is serving. The ``engine`` attribute lands in a later commit; the
        ``getattr`` chain makes this forward-compatible with no change to the body shape.
        """
        rt = getattr(request.app.state, "runtime", None)
        engine = getattr(rt, "engine", None)
        analyzer_ready = bool(engine.ready) if engine is not None else True
        return {"status": "healthy", "analyzer_ready": analyzer_ready}

    return app
