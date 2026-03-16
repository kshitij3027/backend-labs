"""WebSocket connection manager for broadcasting real-time stats."""

import asyncio
import json

from fastapi import WebSocket


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts data to all."""

    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        """Accept and register a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection from the active list."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, data: dict) -> None:
        """Send JSON-encoded data to every active connection.

        Disconnects any client that raises an exception during send.
        """
        payload = json.dumps(data)
        # Iterate over a copy so removals don't mutate the list mid-loop
        for connection in list(self.active_connections):
            try:
                await connection.send_text(payload)
            except Exception:
                self.disconnect(connection)
