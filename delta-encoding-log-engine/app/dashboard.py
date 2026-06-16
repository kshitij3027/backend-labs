"""Live monitoring dashboard: single page, static assets, and a fan-out WebSocket.

This is the dashboard backend. It serves a single-page HTML dashboard at ``/``, its
static assets under ``/static/*`` (mounted in :mod:`app.main`), and pushes live engine
stats over a WebSocket at ``/ws``. The page paints from those WebSocket ticks alone —
there is no polling HTTP from the browser.

**One loop drives every client (single process discipline).** Exactly one background
task — :func:`broadcast_loop`, started in :mod:`app.main`'s ``lifespan`` — wakes every
``dashboard_refresh_ms`` milliseconds, builds one tick, and fans it out to all connected
sockets through the shared :class:`ConnectionManager`. Per-connection handlers do **no**
periodic work; they connect, send one immediate tick so the page paints instantly, then
block on ``receive_text`` purely to detect the client going away. This means N dashboards
cost one stats build per tick, not N — and the heavy stats computation never multiplies
with viewers.

**In-process ticks, never an HTTP self-call.** :func:`build_tick` reads the live object
graph straight off ``app.state`` and calls :func:`~app.api.compose_stats` directly, so a
tick is the same document ``GET /api/stats`` returns without the cost (or deadlock risk)
of the app calling back into itself over HTTP. A failed stats build degrades to
``{"stats": None, "error": <str>}`` rather than killing the loop or the socket.

**Cooperative shutdown, no busy-spin.** The loop sleeps on an :class:`asyncio.Event` via
``asyncio.wait_for(stop_event.wait(), timeout=refresh)``: a :class:`asyncio.TimeoutError`
is the normal "tick now" wake-up, and ``stop_event.set()`` on app teardown wakes it
immediately so it exits promptly with no pending-task warnings. It never spins on a bare
``asyncio.sleep``.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from app.api import compose_stats

# Project root (…/delta-encoding-log-engine). ``app/dashboard.py`` → parent ``app/`` →
# parent is the project root, which is ``/app`` inside the container. Resolving from the
# module file (not CWD) keeps ``/`` working regardless of where uvicorn was launched.
BASE_DIR = Path(__file__).resolve().parent.parent
INDEX_HTML = BASE_DIR / "dashboard" / "templates" / "index.html"

router = APIRouter()


class ConnectionManager:
    """Registry of live dashboard WebSockets with a fail-safe broadcast.

    Single event loop, so a plain :class:`set` needs no lock: ``connect`` accepts and
    registers a socket, ``disconnect`` drops it (idempotent), and :meth:`broadcast`
    sends one JSON message to every current socket. Broadcast is defensive — a client
    that disconnects mid-send is collected and pruned afterwards, and **no** send error
    is ever allowed to propagate out of :meth:`broadcast` (one dead socket must never
    abort the tick for the others).
    """

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """Accept the handshake and register the socket as an active client."""
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Drop ``websocket`` from the active set (safe to call more than once)."""
        self._connections.discard(websocket)

    async def broadcast(self, message: dict) -> None:
        """Send ``message`` as JSON to every connected client; prune any that fail.

        Serializes once, then sends to each socket. A socket that raises mid-send
        (typically because the client just went away) is marked and removed after the
        loop, so the active set self-heals. This method never raises.
        """
        if not self._connections:
            return
        payload = json.dumps(message)
        dead: list[WebSocket] = []
        # Iterate a snapshot: disconnect mutates the set, and a send may prune.
        for websocket in list(self._connections):
            try:
                await websocket.send_text(payload)
            except Exception:  # noqa: BLE001 — a dead socket must not abort the fan-out.
                dead.append(websocket)
        for websocket in dead:
            self._connections.discard(websocket)

    @property
    def count(self) -> int:
        """Number of currently-registered client sockets."""
        return len(self._connections)


def build_tick(app) -> dict:
    """Build one live dashboard tick from the current ``app.state`` graph.

    Reads ``settings`` / ``store`` / ``metrics`` / ``recon_cache`` / ``analyzer`` off
    ``app.state`` and composes the stats document **in-process** via
    :func:`~app.api.compose_stats` (no HTTP self-call), so the tick carries the same
    ``analyzer`` section ``GET /api/stats`` returns. The envelope carries the wall-clock
    ``ts`` and the ``refresh_ms`` cadence so the page can show liveness and self-tune. A
    failed stats build is caught and surfaced as ``{"stats": None, "error": <str>}`` so
    neither the loop nor a socket dies on a transient error — the page shows the error
    instead of going stale silently.
    """
    settings = app.state.settings
    refresh_ms = settings.dashboard_refresh_ms
    try:
        stats = compose_stats(
            app.state.store,
            app.state.metrics,
            app.state.recon_cache,
            app.state.analyzer,
        )
        return {
            "type": "tick",
            "ts": time.time(),
            "refresh_ms": refresh_ms,
            "stats": stats,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 — degrade to an error tick, never raise.
        return {
            "type": "tick",
            "ts": time.time(),
            "refresh_ms": refresh_ms,
            "stats": None,
            "error": str(exc),
        }


async def broadcast_loop(app) -> None:
    """The single background loop: tick every ``dashboard_refresh_ms`` until stopped.

    Reads the manager and the stop event off ``app.state`` (both set up in ``lifespan``
    before this task starts). Each iteration builds one tick and fans it out to every
    client, then sleeps on the stop event with a ``refresh``-second timeout: the
    :class:`asyncio.TimeoutError` is the normal "time for the next tick" wake-up, while a
    set stop event ends the ``await`` immediately so shutdown is prompt and clean. This is
    the only place ticks are produced — there is never more than one of these loops, and
    it never busy-spins on a bare sleep.
    """
    settings = app.state.settings
    manager: ConnectionManager = app.state.ws_manager
    stop_event: asyncio.Event = app.state.ws_stop
    refresh = settings.dashboard_refresh_ms / 1000.0

    while not stop_event.is_set():
        await manager.broadcast(build_tick(app))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=refresh)
        except asyncio.TimeoutError:
            # Normal cadence wake-up: timeout elapsed, loop and tick again.
            pass


@router.get("/", include_in_schema=False)
async def dashboard(request: Request) -> HTMLResponse:
    """Serve the single-page dashboard, read from disk at request time.

    Reading on each request (rather than caching at import) keeps a template edit live
    without a restart in dev, and the path is resolved from :data:`BASE_DIR` so it works
    inside the container where CWD is ``/app``.
    """
    return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))


@router.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    """Stream live engine stats to one dashboard client.

    On connect the client gets one immediate :func:`build_tick` so the page paints at
    once without waiting for the next loop cadence; thereafter the single
    :func:`broadcast_loop` pushes every tick. This handler does no periodic work itself —
    it loops on ``receive_text`` solely to keep the socket open and to notice the client
    going away, unregistering on :class:`WebSocketDisconnect` (or any other receive error)
    so a dead socket is always pruned.
    """
    manager: ConnectionManager = websocket.app.state.ws_manager
    await manager.connect(websocket)
    try:
        # Immediate paint: hand the freshly-connected page a tick right away.
        await websocket.send_text(json.dumps(build_tick(websocket.app)))
        while True:
            # Keep-alive only; inbound messages are ignored. This unblocks on disconnect.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:  # noqa: BLE001 — prune on any receive failure, never propagate.
        manager.disconnect(websocket)
