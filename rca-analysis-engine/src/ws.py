"""WebSocket connection management for real-time incident push (C6).

:class:`ConnectionManager` owns the set of live ``/ws`` clients and fans an incident
report out to every one of them. It is deliberately dependency-light (fastapi only)
and holds no analysis state — the :class:`~src.main.Runtime` constructs a single
manager (C6) and the ``/api/analyze-incident`` handler calls :meth:`broadcast` after
each analysis so connected dashboards update without polling.

Resilience is the whole point of the class: one dead client must never break the
broadcast for the others, and it must never take down the POST that triggered it.
:meth:`broadcast` therefore iterates a *copy* of the active set, swallows a failed
send per-socket, and prunes the offending connections afterwards — the surviving
clients still receive the frame. :meth:`disconnect` is idempotent (``set.discard``)
so the ``/ws`` endpoint can always call it on the way out, whether the client left
cleanly (``WebSocketDisconnect``) or a send failed mid-broadcast.
"""

from __future__ import annotations

import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)

__all__ = ["ConnectionManager"]


class ConnectionManager:
    """Tracks live ``/ws`` clients and broadcasts incident updates to all of them."""

    def __init__(self) -> None:
        #: Live client sockets. A plain set: membership is O(1), order is irrelevant
        #: (every client receives the same frame), and duplicates are impossible.
        self._active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        """Accept the handshake and register the socket as a live client.

        The accept must precede registration — a socket is only broadcast-eligible
        once the ASGI handshake has completed, so a concurrent :meth:`broadcast`
        never tries to send on an un-accepted connection.
        """
        await ws.accept()
        self._active.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a socket from the live set. Idempotent — safe to call twice.

        Uses ``set.discard`` (not ``remove``) so disconnecting an already-removed
        socket — e.g. one pruned mid-broadcast and then again in the endpoint's
        ``finally`` path — is a no-op rather than a ``KeyError``.
        """
        self._active.discard(ws)

    async def broadcast(self, message: dict) -> None:
        """Send ``message`` as JSON to every live client, pruning any that fail.

        Iterates a snapshot (``list(self._active)``) so pruning during the walk can't
        mutate the set under iteration. A send that raises for one socket is caught,
        logged, and that socket is collected; the loop still delivers to the rest.
        Dead sockets are disconnected only after the walk completes, so a single
        broken client can neither abort the broadcast nor starve the others.
        """
        dead: list[WebSocket] = []
        for ws in list(self._active):
            try:
                await ws.send_json(message)
            except Exception:  # noqa: BLE001 - one bad client must not break the rest
                logger.debug("pruning websocket after failed send", exc_info=True)
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def send_personal(self, ws: WebSocket, message: dict) -> None:
        """Send a single JSON ``message`` to one client, pruning it on failure."""
        try:
            await ws.send_json(message)
        except Exception:  # noqa: BLE001 - drop a socket that can no longer be written
            logger.debug("pruning websocket after failed personal send", exc_info=True)
            self.disconnect(ws)

    def count(self) -> int:
        """Return the number of live clients (handy for assertions/observability)."""
        return len(self._active)
