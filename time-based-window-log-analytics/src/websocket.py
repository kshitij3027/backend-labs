"""WebSocket connection manager for live dashboard broadcasting."""

from __future__ import annotations

import json

from fastapi import WebSocket


class ConnectionManager:
    """Manages WebSocket connections for dashboard broadcast."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    async def broadcast(self, data: dict) -> None:
        """Send data to all connected clients, removing dead connections."""
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
