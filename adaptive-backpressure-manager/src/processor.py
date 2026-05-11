import asyncio
import time
from typing import Awaitable, Callable, Dict, Optional

from src.breaker import CircuitBreaker, CircuitOpenError
from src.config import Settings
from src.logging_setup import TAG_ADMIT, get_logger
from src.queues import PriorityQueues
from src.state import Priority


class WorkerPool:
    """N-worker pool draining the priority queues; each item passes through the downstream breaker."""

    def __init__(
        self,
        queues: PriorityQueues,
        breaker: CircuitBreaker,
        settings: Settings,
        downstream: Optional[Callable[[], Awaitable[None]]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._queues = queues
        self._breaker = breaker
        self._settings = settings
        self._downstream = downstream or self._simulate_work
        self._clock = clock
        self._log = get_logger("worker_pool")
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._processed_per_priority: Dict[Priority, int] = {p: 0 for p in Priority}
        self._dropped_per_priority: Dict[Priority, int] = {p: 0 for p in Priority}
        self._errored_per_priority: Dict[Priority, int] = {p: 0 for p in Priority}
        self._processing_lag: float = 0.0
        self._latency_ms: list[float] = []

    @property
    def processed_count(self) -> int:
        return sum(self._processed_per_priority.values())

    @property
    def dropped_count(self) -> int:
        return sum(self._dropped_per_priority.values())

    @property
    def error_count(self) -> int:
        return sum(self._errored_per_priority.values())

    @property
    def processed_per_priority(self) -> Dict[Priority, int]:
        return dict(self._processed_per_priority)

    @property
    def errored_per_priority(self) -> Dict[Priority, int]:
        return dict(self._errored_per_priority)

    @property
    def processing_lag(self) -> float:
        return self._processing_lag

    @property
    def latency_samples_ms(self) -> list[float]:
        return list(self._latency_ms)

    async def _simulate_work(self) -> None:
        await asyncio.sleep(self._settings.processing_latency_seconds)

    def start(self) -> None:
        if self._tasks:
            return
        self._stop.clear()
        for i in range(self._settings.worker_count):
            t = asyncio.create_task(self._worker(i), name=f"worker-{i}")
            self._tasks.append(t)

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _worker(self, worker_id: int) -> None:
        while not self._stop.is_set():
            try:
                priority, item, enq_at = await self._queues.get_next()
            except asyncio.CancelledError:
                raise
            self._processing_lag = max(0.0, self._clock() - enq_at)
            t0 = self._clock()
            try:
                await self._breaker.call(self._downstream)
                self._processed_per_priority[priority] += 1
                self._latency_ms.append((self._clock() - t0) * 1000.0)
                if len(self._latency_ms) > 1000:
                    self._latency_ms = self._latency_ms[-1000:]
            except CircuitOpenError:
                self._errored_per_priority[priority] += 1
            except Exception:
                self._errored_per_priority[priority] += 1
                self._log.exception("worker_error", worker_id=worker_id)
