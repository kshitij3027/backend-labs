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
the app it belongs to. Handlers read shared state defensively off ``request.app.state.runtime``
using ``getattr(..., default)`` and degrade gracefully rather than raising: a missing or
half-wired runtime yields a safe fallback, never a 500.

C9 adds the real-time surface: the permissive :class:`CORSMiddleware` (so the dashboard can
reach the API cross-origin) and the ``/ws`` WebSocket backed by the runtime's
:class:`~src.ws.ConnectionManager`. ``POST /api/analyze`` and ``/api/analyze/batch`` are async
handlers that offload the CPU-bound analysis to the threadpool via ``run_in_threadpool`` (the
event loop stays free) and then **best-effort broadcast** each result — plus a fresh stats
snapshot — to every connected client. The broadcast is fully guarded: a WebSocket failure can
never turn a successful analysis into a failed HTTP response.

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

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool

from src.config import get_settings
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


def _empty_stats_snapshot() -> dict[str, Any]:
    """A well-formed, zeroed ``/api/stats`` body for when no aggregator is wired.

    Mirrors the shape of :meth:`src.stats.StatsAggregator.snapshot` so the endpoint can
    degrade to an empty-but-valid payload (never a 500) if a runtime somehow carries no stats
    aggregator. Kept as a literal here — rather than importing ``StatsAggregator`` — so this
    module stays free of the (transitively heavy) NLP import chain.
    """
    return {
        "total_analyzed": 0,
        "intent_distribution": {},
        "sentiment_distribution": {},
        "entity_type_distribution": {},
        "trending_keywords": [],
        "recent": [],
        "throughput_per_sec": 0.0,
    }


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

    # CORS (C9): installed once so every route — the REST surface and the /ws handshake alike —
    # is reachable cross-origin from the dashboard. Prefer the injected runtime's settings (tests
    # build the Runtime from specific Settings); fall back to the process-wide cached settings on
    # the production/lifespan path. Credentials are disabled whenever "*" is allowed, because the
    # CORS spec forbids pairing the wildcard origin with credentialed requests.
    settings = runtime.settings if runtime is not None else get_settings()
    allow_origins = list(settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials="*" not in allow_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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

    def _record_stats(request: Request, results: list[dict[str, Any]]) -> None:
        """Fold analyze ``results`` into the runtime's rolling stats — a no-op if none is wired.

        Reads the aggregator defensively (``getattr(runtime, "stats", None)``) so a missing or
        half-wired runtime simply skips recording rather than raising; :meth:`StatsAggregator.update`
        is itself robust to any individual malformed result.
        """
        rt = getattr(request.app.state, "runtime", None)
        stats = getattr(rt, "stats", None)
        if stats is None:
            return
        for result in results:
            stats.update(result)

    async def _broadcast(request: Request, results: list[dict[str, Any]]) -> None:
        """Best-effort push of each analysis result + a fresh stats snapshot to ``/ws`` clients.

        Emits one ``{"type": "analysis", "data": <result>}`` frame per result, then a single
        ``{"type": "stats", "data": <snapshot>}`` trailer so the dashboard's aggregate panels
        refresh in lockstep with the feed. Reads the manager (and stats) defensively off the
        runtime, so a missing or half-wired runtime simply skips the push. The whole body is
        wrapped so a broadcast failure can NEVER fail the HTTP response — the analysis is already
        computed and recorded by the time we get here — and :meth:`ConnectionManager.broadcast`
        additionally prunes any individual dead socket internally.
        """
        rt = getattr(request.app.state, "runtime", None)
        manager = getattr(rt, "connections", None)
        if manager is None:
            return
        try:
            for result in results:
                await manager.broadcast({"type": "analysis", "data": result})
            stats = getattr(rt, "stats", None)
            if stats is not None:
                await manager.broadcast({"type": "stats", "data": stats.snapshot()})
        except Exception:  # noqa: BLE001 - broadcasting is best-effort, never fatal
            logger.exception("failed to broadcast analysis frames to /ws clients")

    # NOTE: async `def` handlers (C9). The post-analysis /ws broadcast must run on the event
    # loop, so these are async — but the CPU-bound (GIL-releasing) spaCy/sklearn analysis is
    # pushed to the threadpool via run_in_threadpool so it still never blocks the loop. The fast
    # stats fold and the best-effort broadcast then run inline on the loop after it returns.
    @app.post("/api/analyze", response_model=AnalysisResponse)
    async def analyze(req: AnalyzeRequest, request: Request) -> dict[str, Any]:
        """Analyze one log line into entities, intent, sentiment and keywords.

        Returns the :class:`~src.models.AnalysisResponse` schema. Requires a loaded engine —
        otherwise ``503 analyzer not ready`` (see :func:`_ready_engine`). The successful result
        is folded into the runtime's rolling stats (feeding ``GET /api/stats``) and then
        best-effort broadcast to every ``/ws`` client (an ``analysis`` frame followed by a
        ``stats`` frame). The broadcast can never fail the request.
        """
        engine = _ready_engine(request)
        result = await run_in_threadpool(engine.analyze, req.message)
        _record_stats(request, [result])
        await _broadcast(request, [result])
        return result

    @app.post("/api/analyze/batch", response_model=BatchAnalyzeResponse)
    async def analyze_batch(req: BatchAnalyzeRequest, request: Request) -> dict[str, Any]:
        """Analyze many log lines in one request (order preserved).

        Returns the :class:`~src.models.BatchAnalyzeResponse` envelope (``results`` +
        ``count``). An empty ``messages`` list yields ``{"results": [], "count": 0}``. Requires
        a loaded engine — otherwise ``503 analyzer not ready``. Every result is folded into the
        runtime's rolling stats (feeding ``GET /api/stats``) and then best-effort broadcast to
        every ``/ws`` client (one ``analysis`` frame per result, then a single ``stats`` frame).
        """
        engine = _ready_engine(request)
        results = await run_in_threadpool(engine.analyze_batch, req.messages)
        _record_stats(request, results)
        await _broadcast(request, results)
        return {"results": results, "count": len(results)}

    @app.get("/api/stats")
    def stats(request: Request) -> dict[str, Any]:
        """Return the rolling aggregate stats snapshot powering the dashboard.

        Reads the :class:`~src.stats.StatsAggregator` defensively off the runtime and returns
        its :meth:`~src.stats.StatsAggregator.snapshot`. If no aggregator is wired (a degraded
        or half-built runtime), returns a well-formed **empty** snapshot rather than a 500, so
        the dashboard always gets the documented shape. Never requires a loaded engine.
        """
        rt = getattr(request.app.state, "runtime", None)
        aggregator = getattr(rt, "stats", None)
        if aggregator is None:
            return _empty_stats_snapshot()
        return aggregator.snapshot()

    @app.get("/api/debug/memory")
    def debug_memory() -> dict[str, Any]:
        """Report the process resident-set size in MB (feeds the load-test memory gate).

        Dependency-free and always HTTP 200: :func:`_process_rss_mb` never raises (it degrades
        to ``0.0`` if the RSS cannot be read).
        """
        return {"memory_mb": _process_rss_mb()}

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        """Real-time live feed (C9): register the client, then serve keepalives.

        The connection is registered with the runtime's
        :class:`~src.ws.ConnectionManager` — the same manager ``POST /api/analyze`` broadcasts
        each result + stats snapshot to, so a connected dashboard updates without polling.
        Inbound traffic is used only for a lightweight ``"ping"`` -> ``"pong"`` keepalive; any
        other text is ignored (the client is a listener). The socket is ALWAYS removed from the
        manager on exit — via ``finally`` — whether the client disconnects cleanly
        (:class:`WebSocketDisconnect`) or the receive loop fails for any other reason, so the
        live set never leaks a dead connection. When no runtime/manager is wired (not expected in
        production or under the test fixtures) the handshake is closed immediately rather than
        500-ing.
        """
        rt = getattr(websocket.app.state, "runtime", None)
        manager = getattr(rt, "connections", None)
        if manager is None:
            await websocket.close()
            return
        await manager.connect(websocket)
        try:
            while True:
                text = await websocket.receive_text()
                if text == "ping":
                    await websocket.send_text("pong")
                # Any other inbound message is ignored for now (the client is a listener).
        except WebSocketDisconnect:
            pass
        except Exception:  # noqa: BLE001 - never let a receive-loop error escape unhandled
            logger.debug("websocket receive loop errored; disconnecting", exc_info=True)
        finally:
            manager.disconnect(websocket)

    return app
