from __future__ import annotations

import asyncio
import time
from typing import Callable, Optional

from src.logging_config import get_logger
from src.metrics.ring_buffer import RingBuffer
from src.metrics.sample import MetricSample, StageEvent

_logger = get_logger("metrics.collector")


class MetricsCollector:
    """Receives StageEvents on an asyncio.Queue; joins them with the latest
    ResourceSnapshot for that stage; appends MetricSamples to the RingBuffer
    in batches. Drops-on-full to never back-pressure the hot path.
    """

    def __init__(
        self,
        buffer: RingBuffer,
        batch_size: int,
        resource_lookup: Optional[Callable[[str], "ResourceSnapshot | None"]] = None,
    ) -> None:
        self._buffer = buffer
        self._batch_size = max(1, batch_size)
        self._queue: asyncio.Queue[StageEvent] = asyncio.Queue(maxsize=batch_size * 8)
        self._resource_lookup = resource_lookup
        self._dropped = 0
        self._drop_log_counter = 0

    def set_resource_lookup(self, fn: Callable[[str], "ResourceSnapshot | None"]) -> None:
        self._resource_lookup = fn

    def submit(self, event: StageEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped += 1
            self._drop_log_counter += 1
            if self._drop_log_counter >= 1000:
                _logger.warning("metrics_dropped", dropped_total=self._dropped)
                self._drop_log_counter = 0

    def metrics_dropped(self) -> int:
        return self._dropped

    async def drain_loop(self) -> None:
        """Perpetual task. Cancellation drains remaining events then exits.

        Drains synchronously at startup so an immediate-cancel after submit()
        still flushes pending events (cancellation is delivered at the first
        await, which is after the startup drain).
        """
        self._drain_nowait()
        try:
            while True:
                batch: list[StageEvent] = []
                event = await self._queue.get()
                batch.append(event)
                while len(batch) < self._batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                self._flush_batch(batch)
        except asyncio.CancelledError:
            self._drain_nowait()
            raise

    def _drain_nowait(self) -> None:
        remaining: list[StageEvent] = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            self._flush_batch(remaining)

    def _flush_batch(self, events: list[StageEvent]) -> None:
        now = time.time()
        samples: list[MetricSample] = []
        for evt in events:
            snap = self._resource_lookup(evt.stage) if self._resource_lookup else None
            samples.append(
                MetricSample(
                    stage=evt.stage,
                    ts=now,
                    cpu_pct=snap.cpu_pct if snap else 0.0,
                    mem_mb=snap.mem_mb if snap else 0.0,
                    io_read_bytes=snap.io_read_bytes if snap else 0,
                    io_write_bytes=snap.io_write_bytes if snap else 0,
                    queue_depth=snap.queue_depth if snap else 0,
                    latency_ms=evt.duration_ns / 1_000_000.0,
                )
            )
        self._buffer.extend(samples)
