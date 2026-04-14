"""WebSocket connection manager for real-time alert broadcasting.

The :class:`ConnectionManager` owns the set of currently-connected
WebSocket clients and provides a thread-safe broadcast mechanism.

Design notes:
- A single ``asyncio.Lock`` guards the active-clients set so concurrent
  ``connect``/``disconnect``/``broadcast`` calls don't race.
- The lock is released around per-client ``send_json`` to avoid blocking
  the entire set if one client is slow -- we snapshot the set under the
  lock then iterate the snapshot outside it.
- Dead clients (any exception during ``send_json``) are evicted so a
  single disconnected client never blocks broadcasts.
"""

from __future__ import annotations

import asyncio

import structlog
from fastapi import WebSocket

logger = structlog.get_logger(__name__)


class ConnectionManager:
    """Tracks active WebSocket clients and broadcasts messages to all of them.

    Evicts dead clients on send failure so a single disconnected client
    never blocks the broadcast.
    """

    def __init__(self) -> None:
        self._active: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        """Accept *ws* and add it to the active set."""
        await ws.accept()
        async with self._lock:
            self._active.add(ws)
        logger.info("ws_client_connected", clients=len(self._active))

    async def disconnect(self, ws: WebSocket) -> None:
        """Remove *ws* from the active set (idempotent)."""
        async with self._lock:
            self._active.discard(ws)
        logger.info("ws_client_disconnected", clients=len(self._active))

    async def broadcast_json(self, payload: dict) -> int:
        """Send *payload* as JSON to every active client.

        Returns the number of successful sends.  Any client whose
        ``send_json`` raises is evicted from the active set.
        """
        async with self._lock:
            clients = list(self._active)

        dead: list[WebSocket] = []
        sent = 0

        for ws in clients:
            try:
                await ws.send_json(payload)
                sent += 1
            except Exception as exc:
                logger.debug("broadcast_send_failed", error=str(exc))
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._active.discard(ws)
            logger.info("ws_evicted_dead_clients", count=len(dead))

        return sent

    @property
    def client_count(self) -> int:
        """Current number of connected clients."""
        return len(self._active)
