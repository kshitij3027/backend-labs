"""Async metric collector that periodically polls a NodeSimulator."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from src.models import MetricPoint
from src.simulator import NodeSimulator
from src.storage import MetricStore


class MetricCollector:
    """Async metric collector that periodically polls a NodeSimulator."""

    def __init__(
        self,
        simulator: NodeSimulator,
        store: MetricStore,
        interval: float = 5.0,
        on_new_metrics: Callable[[list[MetricPoint]], Awaitable[None]] | None = None,
    ) -> None:
        self.simulator = simulator
        self.store = store
        self.interval = interval
        self.on_new_metrics = on_new_metrics
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        """Start the collection loop as an asyncio task."""
        self._running = True
        self._task = asyncio.create_task(self._collect_loop())

    async def stop(self) -> None:
        """Stop the collection loop."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _collect_loop(self) -> None:
        """Internal loop: collect -> store -> callback -> sleep."""
        while self._running:
            try:
                points = self.simulator.collect()
                self.store.store(points)
                if self.on_new_metrics:
                    await self.on_new_metrics(points)
            except Exception:
                pass  # Log in production
            await asyncio.sleep(self.interval)
