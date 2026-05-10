import asyncio
from typing import Any, Callable, Dict, Optional, Tuple

from src.config import Settings
from src.logging_setup import TAG_DROP, get_logger
from src.state import Priority


_DRAIN_ORDER = (Priority.CRITICAL, Priority.HIGH, Priority.NORMAL, Priority.LOW)
_BUMP_PAIRS = (
    (Priority.LOW, Priority.NORMAL),
    (Priority.NORMAL, Priority.HIGH),
    (Priority.HIGH, Priority.CRITICAL),
)


class PriorityQueues:
    """Four priority asyncio.Queues with strict-priority drain and anti-starvation bumping."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._queues: Dict[Priority, asyncio.Queue] = {
            Priority.CRITICAL: asyncio.Queue(maxsize=settings.critical_queue_max),
            Priority.HIGH: asyncio.Queue(maxsize=settings.high_queue_max),
            Priority.NORMAL: asyncio.Queue(maxsize=settings.normal_queue_max),
            Priority.LOW: asyncio.Queue(maxsize=settings.low_queue_max),
        }
        self._not_empty = asyncio.Event()
        self._log = get_logger("queues")

    def put_nowait(self, priority: Priority, item: Any, enqueued_at: float) -> None:
        self._queues[priority].put_nowait((item, enqueued_at))
        self._not_empty.set()

    async def get_next(self) -> Tuple[Priority, Any, float]:
        while True:
            for p in _DRAIN_ORDER:
                q = self._queues[p]
                if not q.empty():
                    item, enq = q.get_nowait()
                    if self.total_qsize() == 0:
                        self._not_empty.clear()
                    return p, item, enq
            self._not_empty.clear()
            await self._not_empty.wait()

    def qsize(self, priority: Priority) -> int:
        return self._queues[priority].qsize()

    def total_qsize(self) -> int:
        return sum(q.qsize() for q in self._queues.values())

    def total_depth_ratio(self) -> float:
        total = self.total_qsize()
        cap = self._settings.max_queue_size
        if cap <= 0:
            return 0.0
        ratio = total / cap
        if ratio < 0.0:
            return 0.0
        if ratio > 1.0:
            return 1.0
        return ratio

    def oldest_enqueued_at(self) -> Optional[float]:
        fronts = []
        for q in self._queues.values():
            internal = q._queue
            if internal:
                fronts.append(internal[0][1])
        return min(fronts) if fronts else None

    async def bump_aged_items(
        self, now_fn: Callable[[], float], max_iterations: int = -1
    ) -> int:
        bumped_total = 0
        period = max(0.1, min(self._settings.anti_starvation_age_seconds / 3.0, 1.0))
        i = 0
        while max_iterations < 0 or i < max_iterations:
            i += 1
            await asyncio.sleep(period)
            bumped_total += self._do_bump_pass(now_fn)
        return bumped_total

    def _do_bump_pass(self, now_fn: Callable[[], float]) -> int:
        bumped = 0
        threshold = self._settings.anti_starvation_age_seconds
        now = now_fn()
        for src, dst in _BUMP_PAIRS:
            src_q = self._queues[src]
            dst_q = self._queues[dst]
            while not src_q.empty():
                internal = src_q._queue
                if not internal:
                    break
                _item, enq = internal[0]
                if now - enq < threshold:
                    break
                try:
                    item, enq = src_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    dst_q.put_nowait((item, now))
                    bumped += 1
                except asyncio.QueueFull:
                    self._log.warning(
                        "bump_dest_full",
                        tag=TAG_DROP,
                        reason="starvation_bump_dest_full",
                        from_priority=src.value,
                        to_priority=dst.value,
                    )
        if bumped:
            self._not_empty.set()
        return bumped
