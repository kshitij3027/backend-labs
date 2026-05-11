import asyncio
import time
from typing import Callable, Optional

import psutil

from src.aimd import AIMDLimiter
from src.config import Settings
from src.logging_setup import TAG_PRESSURE, get_logger
from src.metrics_core import PressureFuser, PressureMetrics
from src.queues import PriorityQueues
from src.state import PressureLevel
from src.state_machine import BackpressureManager
from src.upstream_breaker import UpstreamBreaker


class PressureSensor:
    """Background sampler: queues + lag + psutil -> fuser -> state machine + AIMD + upstream breaker."""

    def __init__(
        self,
        queues: PriorityQueues,
        fuser: PressureFuser,
        manager: BackpressureManager,
        aimd: AIMDLimiter,
        upstream: UpstreamBreaker,
        settings: Settings,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._queues = queues
        self._fuser = fuser
        self._manager = manager
        self._aimd = aimd
        self._upstream = upstream
        self._settings = settings
        self._clock = clock
        self._log = get_logger("sensor")
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_level = manager.level
        self._sample_count = 0

    @property
    def sample_count(self) -> int:
        return self._sample_count

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="pressure-sensor")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        interval = self._settings.sampling_interval
        while not self._stop.is_set():
            await self._tick_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def _tick_once(self) -> None:
        m = self._sample()
        score = self._fuser.fuse(m)
        self._upstream.observe(score)
        prev_level = self._manager.level
        prev_limit = self._aimd.limit
        new_level = self._manager.tick(score)
        self._aimd.on_tick()
        if new_level == PressureLevel.OVERLOAD and prev_level != PressureLevel.OVERLOAD:
            self._aimd.on_overload()
        if new_level == PressureLevel.RECOVERY and prev_level == PressureLevel.OVERLOAD:
            self._aimd.on_recovery_entry(prev_limit)
        self._last_level = new_level
        self._sample_count += 1
        self._log.info(
            "sensor_sample",
            tag=TAG_PRESSURE,
            score=score,
            level=new_level.value,
            qdr=m.queue_depth_ratio,
            lag=m.normalized_lag,
            cpu=m.cpu,
            mem=m.mem,
            aimd_limit=self._aimd.limit,
        )

    def _sample(self) -> PressureMetrics:
        now = self._clock()
        qdr = self._queues.total_depth_ratio()
        oldest = self._queues.oldest_enqueued_at()
        if oldest is None:
            lag = 0.0
        else:
            lag = max(0.0, (now - oldest) / max(0.001, self._settings.lag_norm_divisor))
            if lag > 1.0:
                lag = 1.0
        cpu = psutil.cpu_percent(interval=None) / 100.0
        mem = psutil.virtual_memory().percent / 100.0
        return PressureMetrics(queue_depth_ratio=qdr, normalized_lag=lag, cpu=cpu, mem=mem, ts=now)
