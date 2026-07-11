"""FastAPI application factory and HTTP surface for the RCA Analysis Engine.

Endpoints: ``GET /api/health`` — the spec-verbatim liveness probe (C1) — plus the
C5 incident surface: ``POST /api/analyze-incident`` (analyze a posted batch of events
into an :class:`~src.models.IncidentReport`) and ``GET /api/incidents[/{id}]`` (the
bounded, newest-first in-memory incident history and single-incident lookup). The
factory also installs the permissive CORS middleware and the real-time ``/ws``
WebSocket (C6) — the POST handler broadcasts each new report to every connected client
via the :class:`~src.ws.ConnectionManager` — and a later commit adds
``GET /api/calibration`` (C9). Handlers read shared state off
``request.app.state.runtime`` (attached by the lifespan, or injected by tests) and
degrade gracefully when it is absent — reads fall back to empty / 404 and writes to
503, so a missing runtime never becomes a 500.

``/api/health`` always returns HTTP 200 while the process is alive and is fully
dependency-free: the analyzer is in-memory and ready the instant uvicorn binds, so
the body is the exact constant spec contract
``{"status": "healthy", "analyzer_ready": true}``. The unit tests and the C10 E2E
verifier assert it verbatim, so it must never change — the two keys, nothing more.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from src.config import get_settings
from src.models import IncidentReport, LogEvent

if TYPE_CHECKING:
    # Type-only import: src.main imports create_app from this module, so importing
    # Runtime here at runtime would be a circular import.
    from src.main import Runtime

logger = logging.getLogger(__name__)

#: Human-readable API title / version (shown in the OpenAPI docs; not a contract).
API_TITLE = "RCA Analysis Engine"
API_VERSION = "0.1.0"

#: SPEC-VERBATIM /api/health body — never change these keys/values.
_HEALTH_BODY: dict[str, Any] = {"status": "healthy", "analyzer_ready": True}


def _runtime(request: Request) -> Any | None:
    """Return the attached :class:`~src.main.Runtime`, or ``None`` when absent.

    Single defensive accessor for the shared per-process state: ``getattr`` with a
    default tolerates a missing ``state.runtime`` attribute (nothing wired) so callers
    can degrade gracefully rather than raising.
    """
    return getattr(request.app.state, "runtime", None)


def _runtime_analyzer(request: Request) -> Any | None:
    """Return the wired RCA analyzer, or ``None`` when no runtime/analyzer is attached.

    The incident handlers use this to degrade gracefully — reads fall back to ``[]`` /
    404 and writes to 503 — instead of surfacing a 500 when ``app.state.runtime`` (or
    its analyzer) is absent. ``getattr`` with a default tolerates both a missing
    ``state.runtime`` attribute and a ``None`` runtime, mirroring the defensive way the
    rest of the module reads shared app state.
    """
    return getattr(_runtime(request), "analyzer", None)


def _cors_kwargs(cors_origins: str) -> dict[str, Any]:
    """Translate the ``cors_origins`` setting into ``CORSMiddleware`` keyword args.

    A ``"*"`` anywhere in the value means "allow any origin"; the CORS spec forbids
    pairing the ``*`` wildcard with credentialed requests, so credentials are disabled
    in that mode (``allow_origins=["*"]``, ``allow_credentials=False``). Otherwise the
    value is a comma-separated allow-list of explicit origins, which *can* carry
    credentials (``allow_credentials=True``). Methods and headers are always fully
    permissive — this backend is a same-origin-proxied dashboard API, not a public one.
    """
    if "*" in cors_origins:
        allow_origins = ["*"]
        allow_credentials = False
    else:
        allow_origins = [origin.strip() for origin in cors_origins.split(",") if origin.strip()]
        allow_credentials = True
    return {
        "allow_origins": allow_origins,
        "allow_credentials": allow_credentials,
        "allow_methods": ["*"],
        "allow_headers": ["*"],
    }


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

    # CORS: read the allow-list off the injected runtime's settings when present, else
    # the process-wide cached settings (the production/lifespan path builds its Runtime
    # from that same source). Installed once here so every route — the REST surface and
    # the /ws handshake alike — is reachable cross-origin from the :3000 dashboard.
    settings = runtime.settings if runtime is not None else get_settings()
    app.add_middleware(CORSMiddleware, **_cors_kwargs(settings.cors_origins))

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        """Liveness probe — dependency-free, always HTTP 200 while the process is alive.

        The body is a constant spec contract (never derived from runtime state), so
        the probe cannot fail while the process is serving requests. A fresh dict is
        returned each call so the module-level constant can never be mutated.
        """
        return dict(_HEALTH_BODY)

    @app.post("/api/analyze-incident")
    async def analyze_incident(
        events: list[LogEvent], request: Request
    ) -> IncidentReport:
        """Analyze a posted batch of events into a full :class:`IncidentReport`.

        The request body is a top-level JSON array of :class:`~src.models.LogEvent`; a
        malformed element (missing/invalid field) is rejected as 422 by pydantic before
        this handler runs. A :class:`ValueError` raised by the analyzer — e.g. the
        timeline stage rejecting an unparseable ``timestamp`` — is likewise mapped to
        422, so bad input never becomes a 500. When no runtime/analyzer is wired (not
        expected in production or under the test fixtures), the write degrades to 503.

        On success the assembled report is broadcast to every connected ``/ws`` client
        as ``{"type": "incident_update", "data": <report>}`` before it is returned. The
        broadcast is best-effort — a WebSocket failure is logged and swallowed so it can
        never turn a successful analysis into a failed HTTP response.
        """
        runtime = _runtime(request)
        analyzer = getattr(runtime, "analyzer", None)
        if analyzer is None:
            raise HTTPException(status_code=503, detail="analyzer unavailable")
        try:
            report = analyzer.analyze(events)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        # Real-time push (C6): fan the freshly-assembled report out to every live /ws
        # client. Guarded so a broadcast failure never breaks the HTTP response — the
        # report is already computed and stored — while the manager itself also prunes
        # individual dead sockets internally.
        manager = getattr(runtime, "connection_manager", None)
        if manager is not None:
            try:
                await manager.broadcast(
                    {"type": "incident_update", "data": report.model_dump(mode="json")}
                )
            except Exception:  # noqa: BLE001 - broadcasting is best-effort, never fatal
                logger.exception("failed to broadcast incident %s", report.incident_id)
        return report

    @app.get("/api/incidents")
    async def list_incidents(
        request: Request, limit: int | None = None
    ) -> list[IncidentReport]:
        """Return the bounded in-memory incident history, newest-first.

        The optional ``limit`` query param is clamped into
        ``1..settings.max_incident_history`` and returns only the N most recent
        reports. With no analyzer/history wired, an empty list is returned rather than
        an error.
        """
        analyzer = _runtime_analyzer(request)
        if analyzer is None:
            return []
        newest_first = list(reversed(analyzer.incident_history))
        if limit is not None:
            cap = analyzer.settings.max_incident_history
            newest_first = newest_first[: max(1, min(limit, cap))]
        return newest_first

    @app.get("/api/incidents/{incident_id}")
    async def get_incident(incident_id: str, request: Request) -> IncidentReport:
        """Return a single stored report by id, or 404 when no such incident is retained."""
        analyzer = _runtime_analyzer(request)
        if analyzer is not None:
            for report in analyzer.incident_history:
                if report.incident_id == incident_id:
                    return report
        raise HTTPException(
            status_code=404, detail=f"incident {incident_id!r} not found"
        )

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        """Real-time incident feed (C6): register the client, then serve keepalives.

        The connection is registered with the runtime's
        :class:`~src.ws.ConnectionManager`, which is what ``POST /api/analyze-incident``
        broadcasts each new report to. Inbound traffic is only used for a lightweight
        ``"ping"`` -> ``"pong"`` keepalive; any other text is ignored for now. The socket
        is always removed from the manager on exit — whether the client disconnects
        cleanly (:class:`WebSocketDisconnect`) or the receive loop fails for any other
        reason — so the live set never leaks dead connections. When no runtime/manager
        is wired (not expected in production or under the test fixtures) the handshake is
        closed immediately rather than 500-ing.
        """
        rt = getattr(websocket.app.state, "runtime", None)
        manager = getattr(rt, "connection_manager", None)
        if manager is None:
            await websocket.close()
            return
        await manager.connect(websocket)
        try:
            while True:
                text = await websocket.receive_text()
                if text == "ping":
                    await websocket.send_text("pong")
                # Other inbound messages are ignored for now (the client is a listener).
        except WebSocketDisconnect:
            manager.disconnect(websocket)
        except Exception:  # noqa: BLE001 - never let a receive-loop error escape unhandled
            manager.disconnect(websocket)

    return app
