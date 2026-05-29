"""WebSocket connection manager for real-time optimizer/metric streaming.

A single :class:`ConnectionManager` (stashed on ``app.state.ws_manager``) tracks
every dashboard client connected to ``/ws/metrics`` and fans the per-tick
optimizer payload out to all of them. It is intentionally thin glue over the
batcher accessors: the control loop builds a JSON-serialisable dict and calls
:meth:`broadcast`; this layer just serialises once and pushes to each socket,
pruning any that have died so a stale client never breaks the loop or its peers.
"""

from __future__ import annotations

import json

from fastapi import WebSocket


class ConnectionManager:
    """Manage WebSocket connections and broadcast data to all connected clients."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """Accept and register a WebSocket connection."""
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        self._connections.discard(websocket)

    async def broadcast(self, data: dict) -> None:
        """Send JSON data to all connected clients.

        The payload is serialised once (``default=str`` so enums/datetimes fall
        back to their string form), then pushed to every socket. Any connection
        that raises on send is collected and discarded afterwards, so one dead
        client never blocks the broadcast to the others.
        """
        dead: list[WebSocket] = []
        message = json.dumps(data, default=str)
        for ws in self._connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)

    async def send_personal(self, websocket: WebSocket, data: dict) -> None:
        """Send JSON data to a single client.

        Used to push the current state to a freshly connected client right after
        :meth:`connect`, so the dashboard paints immediately instead of waiting a
        full control-loop tick.
        """
        await websocket.send_text(json.dumps(data, default=str))

    @property
    def active_count(self) -> int:
        """Number of currently registered connections."""
        return len(self._connections)
