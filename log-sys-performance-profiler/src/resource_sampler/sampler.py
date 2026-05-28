from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import psutil

from src.logging_config import get_logger
from src.metrics.sample import StageName

_logger = get_logger("resource_sampler")


@dataclass(slots=True)
class ResourceSnapshot:
    stage: StageName
    ts: float
    cpu_pct: float
    mem_mb: float
    io_read_bytes: int
    io_write_bytes: int
    queue_depth: int


class ResourceSampler:
    """Periodically samples process-wide CPU / mem / IO via psutil and stores
    per-stage snapshots (process-wide CPU mirrored across stages because we
    cannot split CPU per-coroutine in a single process — documented).
    """

    def __init__(
        self,
        stages: list[StageName],
        interval_sec: float,
        queue_depth_fn: Optional[Callable[[StageName], int]] = None,
    ) -> None:
        self._stages = stages
        self._interval = max(0.05, interval_sec)
        self._queue_depth_fn = queue_depth_fn or (lambda _s: 0)
        self._latest: Dict[StageName, ResourceSnapshot] = {}
        self._proc = psutil.Process(os.getpid())
        # Prime CPU counter so the first real read is meaningful.
        self._proc.cpu_percent(interval=None)

    def set_queue_depth_fn(self, fn: Callable[[StageName], int]) -> None:
        self._queue_depth_fn = fn

    def latest_for(self, stage: StageName) -> Optional[ResourceSnapshot]:
        return self._latest.get(stage)

    async def sample_loop(self) -> None:
        try:
            while True:
                self._tick()
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            raise

    def _tick(self) -> None:
        try:
            cpu = self._proc.cpu_percent(interval=None)
            mem_mb = self._proc.memory_info().rss / (1024 * 1024)
            try:
                io = self._proc.io_counters()
                read_b = io.read_bytes
                write_b = io.write_bytes
            except (psutil.AccessDenied, AttributeError):
                read_b, write_b = 0, 0
        except psutil.Error as exc:
            _logger.warning("psutil_sample_failed", error=str(exc))
            return
        ts = time.time()
        for stage in self._stages:
            self._latest[stage] = ResourceSnapshot(
                stage=stage,
                ts=ts,
                cpu_pct=cpu,
                mem_mb=mem_mb,
                io_read_bytes=read_b,
                io_write_bytes=write_b,
                queue_depth=int(self._queue_depth_fn(stage)),
            )
