"""WebSocket connection manager + ``/ws`` endpoint.

This module owns the real-time fan-out path to the browser dashboard:

* :class:`ConnectionManager` keeps a registry of connected clients,
  a per-client ``last_pong`` timestamp, and helpers to broadcast
  ``new_document`` events from the index write-path and ``stats_update``
  events from a 1 Hz loop in :mod:`src.main`.
* :func:`register_ws_routes` hangs the ``/ws`` endpoint off a
  FastAPI app, so ``src.main.build_app`` can register the route
  *before* the lifespan runs (at route-time the manager is known).

Event shapes (server -> client)
-------------------------------

* ``{"type": "connected",    "client_id": "abc123…", "server_time": 12.3}``
* ``{"type": "new_document", "document": {…LogEntry dump…}}``
* ``{"type": "stats_update", "data": {…StatsResponse-shaped…}}``
* ``{"type": "ping",         "t": 12.3}``

Event shapes (client -> server)
-------------------------------

* ``{"type": "pong", "t": 12.3}`` — the dashboard replies to every
  ``ping`` so stale-eviction knows the socket is alive.

Concurrency model
-----------------

A single :class:`asyncio.Lock` guards ``_clients`` / ``_last_pong``
for writes. Reads (``active_count``) are racy-but-safe: they can only
over- or under-count by at most one, which is fine for operational
metrics. Broadcast copies the client dict under the lock, then sends
outside the lock so a slow socket can't stall other clients or the
write-path callback.

Dead clients are collected during each broadcast and evicted
afterwards; never mutate ``_clients`` while iterating it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from src.models import LogEntry


logger = logging.getLogger(__name__)


class ConnectionManager:
    """Tracks connected WebSocket clients and broadcasts events.

    One instance is built in :func:`src.main.build_app` and stashed on
    ``app.state.ws_manager``. The :class:`InvertedIndex` uses
    :meth:`broadcast_new_document` as its ``on_new_document`` callback
    so every indexed document fans out to all connected dashboards;
    the lifespan also spins a 1 Hz loop that calls
    :meth:`broadcast_stats` with the latest stats snapshot.
    """

    def __init__(self) -> None:
        self._clients: dict[str, WebSocket] = {}
        self._last_pong: dict[str, float] = {}
        self._lock: asyncio.Lock = asyncio.Lock()
        self._closed: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, websocket: WebSocket) -> str:
        """Accept *websocket*, assign a short client id, send ``connected``.

        The id is a 12-char hex prefix of a UUID — plenty of entropy for
        per-process uniqueness without making log lines unreadable.
        """
        await websocket.accept()
        client_id = uuid.uuid4().hex[:12]
        async with self._lock:
            self._clients[client_id] = websocket
            self._last_pong[client_id] = time.time()
        logger.info(
            "ws client %s connected (total=%d)", client_id, len(self._clients)
        )
        await self._send_safe(
            client_id,
            {
                "type": "connected",
                "client_id": client_id,
                "server_time": time.time(),
            },
        )
        return client_id

    async def disconnect(self, client_id: str) -> None:
        """Drop *client_id* from the registry and best-effort close it.

        Idempotent — calling on an already-removed id is a no-op, which
        matters because the endpoint's ``finally`` block and the
        broadcast dead-sweep can both reach here for the same id.
        """
        async with self._lock:
            ws = self._clients.pop(client_id, None)
            self._last_pong.pop(client_id, None)
        if ws is not None:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001 — close is best-effort
                pass
            logger.info(
                "ws client %s disconnected (total=%d)",
                client_id,
                len(self._clients),
            )

    async def close_all(self) -> None:
        """Drop every client; used during lifespan shutdown."""
        async with self._lock:
            ids = list(self._clients.keys())
        for cid in ids:
            await self.disconnect(cid)
        self._closed = True

    def active_count(self) -> int:
        """Return the current client count (racy but safe for metrics)."""
        return len(self._clients)

    # ------------------------------------------------------------------
    # Broadcast API — called from index + stats loop
    # ------------------------------------------------------------------

    async def broadcast_new_document(self, entry: LogEntry) -> None:
        """Fan ``new_document`` out to every connected dashboard.

        Wired as :class:`InvertedIndex`'s ``on_new_document`` callback;
        fires once per added doc from a fire-and-forget task, so a
        slow socket never blocks ingest.
        """
        await self._broadcast(
            {"type": "new_document", "document": entry.model_dump()}
        )

    async def broadcast_stats(self, stats: dict) -> None:
        """Fan a ``stats_update`` snapshot out to every client.

        ``stats`` is already a flat dict in the :class:`StatsResponse`
        shape; we wrap it rather than send it raw so the client can
        distinguish event types.
        """
        await self._broadcast({"type": "stats_update", "data": stats})

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def heartbeat_loop(
        self, interval: float, stop_event: asyncio.Event
    ) -> None:
        """Send a ``ping`` every *interval* seconds and evict stale clients.

        A client is considered stale if its ``last_pong`` is older than
        three heartbeat intervals — that's generous enough to tolerate
        packet loss on a flaky link without silently accumulating
        zombie connections. The loop honours *stop_event* via
        ``asyncio.wait_for`` so shutdown is prompt even mid-interval.
        """
        while not stop_event.is_set():
            try:
                await self._broadcast({"type": "ping", "t": time.time()})

                # Evict anyone who hasn't pong'd in 3 intervals. We
                # collect the list under the lock, then disconnect
                # outside the lock (disconnect takes the lock itself
                # and we want the broadcast path to be re-entrant-free).
                threshold = time.time() - 3 * interval
                stale: list[str] = []
                async with self._lock:
                    for cid, t in list(self._last_pong.items()):
                        if t < threshold:
                            stale.append(cid)
                for cid in stale:
                    logger.info("evicting stale ws client %s", cid)
                    await self.disconnect(cid)
            except Exception as exc:  # noqa: BLE001 — never die silently
                logger.exception("heartbeat error: %s", exc)

            # Interruptible sleep so teardown doesn't wait a full
            # interval before noticing the stop signal.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _broadcast(self, payload: dict) -> None:
        """Send *payload* to every connected client; reap failed sockets.

        The client dict is snapshot-copied under the lock, then
        iterated outside the lock so a slow ``send_text`` on one
        socket can't stall writes to the others. Any socket that
        raises during send is queued for disconnect afterwards — we
        never mutate ``_clients`` while iterating it.
        """
        if not self._clients:
            return
        message = json.dumps(payload)
        async with self._lock:
            items = list(self._clients.items())
        dead: list[str] = []
        for cid, ws in items:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001 — socket failure classes vary
                dead.append(cid)
        for cid in dead:
            await self.disconnect(cid)

    async def _send_safe(self, client_id: str, payload: dict) -> None:
        """Send a single payload to one client; disconnect on failure."""
        ws = self._clients.get(client_id)
        if ws is None:
            return
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:  # noqa: BLE001
            await self.disconnect(client_id)


# ---------------------------------------------------------------------------
# Route registration helper
# ---------------------------------------------------------------------------

def register_ws_routes(app: FastAPI, manager: ConnectionManager) -> None:
    """Attach the ``/ws`` endpoint to *app*, wired to *manager*.

    Called from :func:`src.main.build_app` — registering at route-time
    (rather than during the lifespan) means the endpoint is visible to
    the OpenAPI schema and to tests that bypass the lifespan.

    The endpoint's responsibilities:

    * :meth:`ConnectionManager.connect` accepts, assigns a client id,
      and sends the ``connected`` welcome.
    * Loop on ``receive_text`` — the only message we expect from the
      client is ``{"type":"pong"}`` which refreshes ``last_pong``.
      Everything else is parsed defensively and silently ignored.
    * On disconnect or any exception, the ``finally`` block removes
      the client from the manager.
    """

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        client_id = await manager.connect(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                try:
                    msg = json.loads(data)
                except Exception:  # noqa: BLE001 — malformed JSON, drop silently
                    continue
                if not isinstance(msg, dict):
                    continue
                if msg.get("type") == "pong":
                    async with manager._lock:
                        if client_id in manager._last_pong:
                            manager._last_pong[client_id] = time.time()
        except WebSocketDisconnect:
            # Clean client-initiated close; nothing to log beyond the
            # disconnect path itself.
            pass
        except Exception as exc:  # noqa: BLE001
            logger.warning("ws endpoint error for %s: %s", client_id, exc)
        finally:
            await manager.disconnect(client_id)
