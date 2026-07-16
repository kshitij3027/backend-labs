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
import resource
import sys
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request

from src.models import (
    AnalysisResponse,
    AnalyzeRequest,
    BatchAnalyzeRequest,
    BatchAnalyzeResponse,
)

if TYPE_CHECKING:
    # Type-only import: src.main imports create_app from this module, so importing Runtime
    # at runtime here would be a circular import.
    from src.main import Runtime

logger = logging.getLogger(__name__)

#: Human-readable API title / version (shown in the OpenAPI docs; not a contract).
API_TITLE = "NLP Log Processing Engine"
API_VERSION = "0.1.0"


def _process_rss_mb() -> float:
    """Return this process's resident-set size in MB. Never raises — falls back to 0.0.

    Primary source is ``/proc/self/status`` ``VmRSS`` (kB) — accurate and current on Linux
    (the container). When ``/proc`` is unavailable (macOS dev boxes) it falls back to
    :func:`resource.getrusage` ``ru_maxrss``, whose unit differs by platform: **bytes** on
    macOS, **kB** on Linux. Any failure degrades to ``0.0`` rather than propagating.
    """
    try:
        with open("/proc/self/status", encoding="ascii") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return round(float(line.split()[1]) / 1024.0, 2)
    except OSError:
        pass
    try:
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
        return round(maxrss / divisor, 2)
    except (ValueError, OSError):
        return 0.0


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

    def _ready_engine(request: Request) -> Any:
        """Return the loaded NLP engine or raise 503 — the shared guard for the analyze routes.

        Reads the engine defensively off the runtime (a missing runtime/engine degrades to
        ``None`` rather than raising), and turns "no engine wired" or "engine not yet loaded"
        into a clean ``503 analyzer not ready`` — never a 500.
        """
        rt = getattr(request.app.state, "runtime", None)
        engine = getattr(rt, "engine", None)
        if engine is None or not engine.ready:
            raise HTTPException(status_code=503, detail="analyzer not ready")
        return engine

    # NOTE: sync `def` handlers on purpose. FastAPI runs sync routes in a threadpool, so the
    # (CPU-bound, GIL-releasing) spaCy/sklearn work never blocks the event loop.
    @app.post("/api/analyze", response_model=AnalysisResponse)
    def analyze(req: AnalyzeRequest, request: Request) -> dict[str, Any]:
        """Analyze one log line into entities, intent, sentiment and keywords.

        Returns the :class:`~src.models.AnalysisResponse` schema. Requires a loaded engine —
        otherwise ``503 analyzer not ready`` (see :func:`_ready_engine`).
        """
        engine = _ready_engine(request)
        return engine.analyze(req.message)

    @app.post("/api/analyze/batch", response_model=BatchAnalyzeResponse)
    def analyze_batch(req: BatchAnalyzeRequest, request: Request) -> dict[str, Any]:
        """Analyze many log lines in one request (order preserved).

        Returns the :class:`~src.models.BatchAnalyzeResponse` envelope (``results`` +
        ``count``). An empty ``messages`` list yields ``{"results": [], "count": 0}``. Requires
        a loaded engine — otherwise ``503 analyzer not ready``.
        """
        engine = _ready_engine(request)
        results = engine.analyze_batch(req.messages)
        return {"results": results, "count": len(results)}

    @app.get("/api/debug/memory")
    def debug_memory() -> dict[str, Any]:
        """Report the process resident-set size in MB (feeds the load-test memory gate).

        Dependency-free and always HTTP 200: :func:`_process_rss_mb` never raises (it degrades
        to ``0.0`` if the RSS cannot be read).
        """
        return {"memory_mb": _process_rss_mb()}

    return app
