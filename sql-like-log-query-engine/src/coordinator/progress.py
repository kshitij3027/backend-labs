"""Per-query progress event bus.

``ProgressEmitter`` is a thin wrapper around an ``asyncio.Queue`` that the
executor pushes ``ProgressEvent``s into and a WebSocket route (arriving in
Commit 7) drains via the async iterator. The queue is unbounded and event
payloads are small, so backpressure is a non-issue.

``ProgressRegistry`` maps ``query_id`` to emitter so a streaming route can
discover the emitter for a query kicked off by a separate request.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

from src.shared.models import ProgressEvent


# Sentinel pushed onto the queue when the producer is done.
_SENTINEL: object = object()


class ProgressEmitter:
    """Produce/consume ``ProgressEvent``s through a single asyncio queue."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self._closed: bool = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def emit(self, event: ProgressEvent) -> None:
        """Push one event onto the queue. No-op once ``close`` has run."""

        if self._closed:
            return
        await self._queue.put(event)

    async def close(self) -> None:
        """Signal that no more events will arrive.

        Safe to call more than once — the second call is a no-op.
        """

        if self._closed:
            return
        self._closed = True
        await self._queue.put(_SENTINEL)

    async def iter(self) -> AsyncIterator[ProgressEvent]:
        """Yield events until ``close`` is called."""

        while True:
            item = await self._queue.get()
            if item is _SENTINEL:
                return
            assert isinstance(item, ProgressEvent)
            yield item


class ProgressRegistry:
    """Module-level registry mapping ``query_id`` to ``ProgressEmitter``.

    A single coordinator process only needs one of these — it lives on
    ``app.state`` in production, but tests may use fresh instances.
    """

    def __init__(self) -> None:
        self._emitters: dict[str, ProgressEmitter] = {}
        self._lock = asyncio.Lock()

    async def create(self, query_id: str) -> ProgressEmitter:
        emitter = ProgressEmitter()
        async with self._lock:
            self._emitters[query_id] = emitter
        return emitter

    def get(self, query_id: str) -> ProgressEmitter | None:
        return self._emitters.get(query_id)

    async def remove(self, query_id: str) -> ProgressEmitter | None:
        async with self._lock:
            return self._emitters.pop(query_id, None)


# A default module-level registry for callers who don't want to pass one.
_default_registry = ProgressRegistry()


def default_registry() -> ProgressRegistry:
    """Return the process-wide default progress registry."""

    return _default_registry
