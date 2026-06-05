"""WebSocket connection manager for the live monitoring dashboard.

A single :class:`ConnectionManager` (stashed on ``app.state.ws_manager`` by the
lifespan in :mod:`src.main`) tracks every dashboard client connected to ``/ws``
and fans the per-tick metrics payload out to all of them.

The broadcast loop in ``main.py`` builds a JSON-serialisable dict
(``{"type": "tick", "stats", "series", "tenants", "migrations", "indexes",
"tiers"}``) every ``ws_push_interval_seconds`` and calls :meth:`broadcast`; this
layer just pushes to each socket, pruning any that have died so a stale client
never breaks the loop or its peers. Each freshly connected client also gets an
immediate :meth:`send_personal` push so the dashboard paints without waiting a
full tick.
"""

from __future__ import annotations

from fastapi import WebSocket

__all__ = ["ConnectionManager"]


class ConnectionManager:
    """Manage WebSocket connections and broadcast metrics to all clients.

    Connections are held in a plain ``set``; broadcasts iterate over a snapshot
    of that set so a client pruned mid-iteration (because its send raised) never
    mutates the structure being walked. Every send is wrapped so one dead client
    is silently discarded rather than aborting the fan-out.
    """

    def __init__(self) -> None:
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        """Accept the handshake and register the connection."""
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a connection (no-op if it was already pruned)."""
        self.active.discard(ws)

    async def broadcast(self, message: dict) -> None:
        """Send ``message`` as JSON to every connected client.

        Iterates over a ``list(self.active)`` snapshot so pruning is safe, and
        catches **any** exception from a send — that socket is discarded and the
        broadcast continues to the remaining clients. Never raises.
        """
        for ws in list(self.active):
            try:
                await ws.send_json(message)
            except Exception:  # noqa: BLE001 — prune dead clients, never abort
                self.active.discard(ws)

    async def send_personal(self, ws: WebSocket, message: dict) -> None:
        """Send ``message`` as JSON to a single client; prune it on failure.

        Used for the immediate push right after :meth:`connect` so a new
        dashboard renders current state instantly. A failed send discards the
        socket rather than raising.
        """
        try:
            await ws.send_json(message)
        except Exception:  # noqa: BLE001 — prune on failure, never raise
            self.active.discard(ws)
