"""FastAPI app factory + routes + WS broadcaster for the dashboard.

Wires every piece built in commits 1-3 into a single ASGI app:

* ``GET /``                — serves the raw Vue+Tailwind dashboard.
* ``GET /api/health``      — :class:`HealthSnapshot` JSON.
* ``GET /api/status``      — alias for ``/api/health`` (dashboard polling
  fallback when the WebSocket is unavailable).
* ``POST /api/logs``       — body :class:`LogWriteRequest` → routes
  through the :class:`ReplicationController` so the entry lands in the
  primary and fans out to every secondary.
* ``GET  /api/logs``       — recent entries from the **primary**'s log
  store, capped by ``MAX_LOGS_RETURNED`` (override via ``?limit=``).
* ``WS  /ws``              — every connected client receives a fresh
  ``HealthSnapshot`` JSON immediately on connect, and another every
  ``WEBSOCKET_PUSH_INTERVAL_SEC`` seconds thereafter.

Patterns reused / adapted from
``active-passive-failover-log-processor/src/dashboard.py``:

* The connection-set + ``send_text`` broadcast loop. Adapted, not
  copied verbatim — that file polls remote peers; we read a single
  in-process :class:`HealthMonitor`.
* The ``HTMLResponse(content=path.read_text())`` pattern for serving
  raw HTML so Vue's ``{{ }}`` template syntax isn't intercepted by a
  templating engine.

Failover-related endpoints (``/api/regions/{id}/kill`` and friends)
deliberately land in commit 5; see ``plan.md`` lines 348-353.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Dict, Optional, Set

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, JSONResponse

from .config import AppConfig
from .health_monitor import HealthMonitor
from .models import LogWriteRequest
from .region import Region
from .replication_controller import ReplicationController
from .replication_stats import ReplicationStatsTracker

# The dashboard HTML lives next to ``src/`` at ``<repo>/web/index.html``.
# Resolve once at import time so we don't repeatedly join paths per
# request — the contents are re-read on each ``GET /`` so changes during
# development don't require a restart.
WEB_INDEX = Path(__file__).resolve().parent.parent / "web" / "index.html"


class WSBroadcaster:
    """Pushes :class:`HealthSnapshot` JSON to every connected WS client.

    On connect we send an immediate snapshot (so the dashboard doesn't
    have to wait a full ``push_interval_sec`` for first paint) and then
    a background task fans out a fresh snapshot every interval to the
    full connection set.

    Failed sends are silently dropped — the originating socket is
    discarded from the connection set on next iteration. WS clients
    don't send messages to us (the dashboard is read-only), but we
    block on ``receive_text`` in the route handler to keep the
    connection open until the client closes it.
    """

    def __init__(self, monitor: HealthMonitor, push_interval_sec: float) -> None:
        self._monitor: HealthMonitor = monitor
        self._interval: float = push_interval_sec
        self._connections: Set[WebSocket] = set()
        self._task: Optional[asyncio.Task[None]] = None

    async def connect(self, ws: WebSocket) -> None:
        """Accept a new WS connection and send an immediate snapshot."""
        await ws.accept()
        self._connections.add(ws)
        # Send an immediate snapshot so the client doesn't wait the full
        # push interval (5s by default) for first paint. If the send
        # fails the socket clearly isn't usable; drop it.
        try:
            await ws.send_text(self._monitor.get_snapshot().model_dump_json())
        except Exception:
            self._connections.discard(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a client from the broadcast set (idempotent)."""
        self._connections.discard(ws)

    async def start(self) -> None:
        """Start the background broadcast task (idempotent)."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the broadcast task and wait for clean exit."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        """Push the latest snapshot to every connection on a fixed cadence."""
        while True:
            try:
                if self._connections:
                    payload = self._monitor.get_snapshot().model_dump_json()
                    dead: list[WebSocket] = []
                    # ``list(self._connections)`` snapshots the set so we
                    # can mutate it (via ``dead`` cleanup) without
                    # ``RuntimeError: Set changed size during iteration``.
                    for ws in list(self._connections):
                        try:
                            await ws.send_text(payload)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        self._connections.discard(ws)
            except Exception:
                # The broadcaster must never crash — the dashboard is
                # observability infrastructure and a thrown exception
                # here would silently freeze every client.
                pass
            await asyncio.sleep(self._interval)


def create_app(config: AppConfig) -> FastAPI:
    """Build the FastAPI app and all in-process region machinery.

    Construction order matters — :class:`ReplicationController`
    elects a primary at ``__init__`` time, so we have to build the
    regions and the stats tracker first. The HealthMonitor and
    WSBroadcaster start their background tasks inside the
    :func:`lifespan` async context manager, which fires when ``uvicorn``
    (or ``TestClient``) drives app startup.

    Everything is stashed on ``app.state`` so tests (and commit 5's
    failover endpoints) can reach into the live cluster without
    re-constructing it.
    """
    # 1. Build the in-process region cluster.
    regions: Dict[str, Region] = {rid: Region(rid) for rid in config.regions}
    stats = ReplicationStatsTracker(regions=list(regions.keys()))
    controller = ReplicationController(
        regions=regions,
        primary_preference=config.primary_preference,
        stats=stats,
    )
    monitor = HealthMonitor(
        regions=regions,
        controller=controller,
        stats=stats,
        check_interval_sec=config.health_check_interval_sec,
    )
    broadcaster = WSBroadcaster(
        monitor=monitor,
        push_interval_sec=config.websocket_push_interval_sec,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Spin up background tasks on startup; cancel + await them on
        # shutdown so pytest's TestClient teardown doesn't leak loops.
        await monitor.start()
        await broadcaster.start()
        try:
            yield
        finally:
            await broadcaster.stop()
            await monitor.stop()

    app = FastAPI(title="Multi-Region Log Replication", lifespan=lifespan)

    # Expose the live state for tests + commit 5 endpoints.
    app.state.config = config
    app.state.regions = regions
    app.state.controller = controller
    app.state.stats = stats
    app.state.monitor = monitor
    app.state.broadcaster = broadcaster

    # =====================================================================
    # GET / — raw HTML so Vue's ``{{ }}`` template syntax works.
    # =====================================================================
    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        # Re-read on each request so dashboard tweaks during dev don't
        # require a restart. Cost is negligible (a small file read).
        return HTMLResponse(content=WEB_INDEX.read_text(), status_code=200)

    # =====================================================================
    # GET /api/health — top-level cluster health snapshot.
    # =====================================================================
    @app.get("/api/health")
    async def health() -> JSONResponse:
        return JSONResponse(content=monitor.get_snapshot().model_dump())

    # =====================================================================
    # GET /api/status — alias for /api/health.
    # =====================================================================
    # Kept as a separate route (rather than redirecting) so the dashboard's
    # polling fallback is symmetric with the WebSocket payload.
    @app.get("/api/status")
    async def status() -> JSONResponse:
        return JSONResponse(content=monitor.get_snapshot().model_dump())

    # =====================================================================
    # POST /api/logs — write through the primary, fan out to secondaries.
    # =====================================================================
    @app.post("/api/logs")
    async def write_log(req: LogWriteRequest) -> dict:
        try:
            entry = await controller.write(req.model_dump())
        except RuntimeError as e:
            # ``current_primary()`` raises RuntimeError when no primary
            # is elected (e.g. all regions unhealthy). 503 is the right
            # signal for "service temporarily unable to accept writes".
            raise HTTPException(status_code=503, detail=str(e))
        return {
            "log_id": entry.log_id,
            "region": entry.region,
            "vector_clock": entry.vector_clock,
            "logical_ts": entry.logical_ts,
            "created_at": entry.created_at,
        }

    # =====================================================================
    # GET /api/logs — recent entries from the current primary.
    # =====================================================================
    @app.get("/api/logs")
    async def list_logs(limit: int = Query(default=None, ge=1, le=10_000)) -> list:
        # Default cap from config; per-request override via ``?limit=``.
        n = limit if limit is not None else config.max_logs_returned
        try:
            primary = controller.current_primary()
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        entries = primary.get_logs(limit=n)
        return [e.model_dump() for e in entries]

    # =====================================================================
    # POST /api/regions/{region_id}/kill — simulate a region failure.
    # =====================================================================
    # Gated by ``config.allow_kill_endpoint`` so production deployments can
    # disable it. Used by the E2E driver and the dashboard's "Kill" button.
    @app.post("/api/regions/{region_id}/kill")
    async def kill_region(region_id: str) -> dict:
        if not config.allow_kill_endpoint:
            raise HTTPException(
                status_code=403, detail="kill endpoint disabled"
            )
        region = regions.get(region_id)
        if region is None:
            raise HTTPException(
                status_code=404, detail=f"region {region_id} not found"
            )
        region.mark_offline()
        return {"region_id": region_id, "is_healthy": False}

    # =====================================================================
    # POST /api/regions/{region_id}/heal — recover a previously-killed region.
    # =====================================================================
    # Heal is intentionally ungated — the failover behaviour we want is
    # one-way (heal does NOT auto-promote), so heal alone is harmless.
    @app.post("/api/regions/{region_id}/heal")
    async def heal_region(region_id: str) -> dict:
        region = regions.get(region_id)
        if region is None:
            raise HTTPException(
                status_code=404, detail=f"region {region_id} not found"
            )
        region.mark_online()
        return {"region_id": region_id, "is_healthy": True}

    # =====================================================================
    # GET /api/regions/{region_id}/logs — read logs from a specific region.
    # =====================================================================
    # Proves that secondary reads still work after a primary failure
    # (used by the verify_replication.py E2E script).
    @app.get("/api/regions/{region_id}/logs")
    async def list_region_logs(
        region_id: str,
        limit: int = Query(default=None, ge=1, le=10_000),
    ) -> list:
        region = regions.get(region_id)
        if region is None:
            raise HTTPException(
                status_code=404, detail=f"region {region_id} not found"
            )
        n = limit if limit is not None else config.max_logs_returned
        entries = region.get_logs(limit=n)
        return [e.model_dump() for e in entries]

    # =====================================================================
    # WS /ws — broadcaster connect endpoint.
    # =====================================================================
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket) -> None:
        await broadcaster.connect(ws)
        try:
            # We don't expect any client-to-server frames; the await
            # blocks until the client closes, at which point
            # ``WebSocketDisconnect`` propagates and we clean up.
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            # Any other unexpected error: clean up and exit; the client
            # will reconnect via the HTML's reconnect loop.
            pass
        finally:
            broadcaster.disconnect(ws)

    return app
