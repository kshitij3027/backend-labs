"""WebSocket connection manager for real-time metric streaming."""

from __future__ import annotations

import json

from fastapi import WebSocket


class ConnectionManager:
    """Manages WebSocket connections and broadcasts data to all connected clients."""

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

        Removes dead connections on send failure.
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

    @property
    def active_count(self) -> int:
        return len(self._connections)
