"""Request models, counters, and the backpressure-aware ingest pipeline.

Commit 6 scope: a bounded ``asyncio.Queue`` sits between the event
producers (:class:`LogEventGenerator` and ``POST /api/metric``) and the
synchronous :class:`WindowManager`. A dedicated consumer task drains
the queue into ``window_manager.dispatch``.

Two overload behaviours kick in when the queue gets hot:

* **Drop-oldest overflow** — once the queue is at capacity, the oldest
  event is evicted to make room for the new one. ``dropped_count`` is
  incremented for every eviction. Producers are never blocked.
* **Adaptive sampling (generator only)** — once the queue crosses the
  80% high-water mark, the generator-side sink samples 1-in-2 events.
  Above 95% the sampler escalates to 1-in-4. User-submitted events via
  ``submit_user`` are **never** sampled, so operator-driven ingest
  always wins over synthetic load.

Counters (``enqueued``, ``dropped``, ``sampled``, ``processed``) are
exposed via :meth:`IngestPipeline.snapshot`, which is embedded in
``GET /api/stats`` and the WebSocket broadcast payload.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

from src.models import Event
from src.window_manager import WindowManager

logger = logging.getLogger(__name__)


class MetricRequest(BaseModel):
    """Validated shape for a metric event posted via HTTP.

    Attributes:
        metric: Metric name (non-empty, capped at 64 chars to avoid abuse).
        value: Numeric measurement.
        metadata: Optional free-form key/value metadata.
    """

    metric: str = Field(..., min_length=1, max_length=64)
    value: float
    metadata: dict[str, Any] = Field(default_factory=dict)


@dataclass
class IngestCounters:
    """Running totals for the ingestion pipeline's health.

    ``enqueued`` counts every event that *actually made it onto* the
    queue — it does **not** include events rejected by the adaptive
    sampler (those are counted by ``sampled`` instead). ``dropped``
    counts events evicted by the drop-oldest policy, which happens
    *after* the decision to enqueue.
    """

    enqueued: int = 0
    dropped: int = 0
    sampled: int = 0
    processed: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "enqueued": self.enqueued,
            "dropped": self.dropped,
            "sampled": self.sampled,
            "processed": self.processed,
        }


class IngestPipeline:
    """Bounded async queue between event producers and the window manager.

    Provides drop-oldest overflow handling plus an adaptive 1-in-N
    sampler that kicks in when the queue depth exceeds 80% capacity
    (only the sampled path is subject to it — user-submitted events via
    :meth:`submit_user` always enter the queue regardless of depth).
    """

    _HIGH_WATER_RATIO = 0.80  # sampler kicks in at 80% full
    _ESCALATE_RATIO = 0.95    # escalate to heavy sampling at 95% full
    _SAMPLE_LIGHT = 2         # 1-in-2 when over high water
    _SAMPLE_HEAVY = 4         # 1-in-4 when still over after escalation

    def __init__(self, window_manager: WindowManager, maxsize: int = 10000) -> None:
        self._window_manager = window_manager
        self._maxsize = maxsize
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self._counters = IngestCounters()
        # Deterministic tick for 1-in-N sampling. Using a counter (as
        # opposed to random.random()) gives us predictable coverage in
        # unit tests and a stable hit-rate under load.
        self._sampler_counter = 0

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def counters(self) -> IngestCounters:
        return self._counters

    def snapshot(self) -> dict[str, int]:
        """Return a snapshot suitable for embedding in ``/api/stats``.

        Includes the four running counters plus a live view of the
        queue depth and its configured ``maxsize``.
        """
        s = self._counters.snapshot()
        s["queue_depth"] = self.queue_depth
        s["queue_maxsize"] = self._maxsize
        return s

    async def submit_generated(self, event: Event) -> None:
        """Producer sink for the generator — subject to adaptive sampling.

        If the queue is below the 80% high-water mark, the event is
        forwarded straight to :meth:`_put_with_overflow`. Above that
        mark, a deterministic 1-in-N sampler drops most events so the
        queue can drain; the rate escalates to 1-in-4 once the queue
        is above 95% full.
        """
        depth = self._queue.qsize()
        high_water = int(self._maxsize * self._HIGH_WATER_RATIO)
        if depth >= high_water:
            escalate = int(self._maxsize * self._ESCALATE_RATIO)
            n = self._SAMPLE_HEAVY if depth >= escalate else self._SAMPLE_LIGHT
            self._sampler_counter += 1
            # Keep 1-in-N (every Nth tick passes through), drop the rest.
            if self._sampler_counter % n != 0:
                self._counters.sampled += 1
                return
        await self._put_with_overflow(event)

    async def submit_user(self, event: Event) -> None:
        """Producer sink for user-submitted events — never sampled.

        Operator-driven ingest is privileged: we always try to enqueue
        the event, falling back on the drop-oldest policy if the queue
        is already at capacity.
        """
        await self._put_with_overflow(event)

    async def _put_with_overflow(self, event: Event) -> None:
        """Drop-oldest policy: evict head if full, then append.

        Always non-blocking — producers are never paused. If the queue
        is at capacity we pop the head (oldest event), increment the
        dropped counter, and then enqueue the new event.
        """
        if self._queue.full():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._counters.dropped += 1
            except asyncio.QueueEmpty:  # pragma: no cover - race-window defensive
                pass
        self._queue.put_nowait(event)
        self._counters.enqueued += 1

    async def run_consumer(self, stop_event: asyncio.Event) -> None:
        """Drain the queue into the :class:`WindowManager` until stopped.

        Uses :func:`asyncio.wait_for` on ``queue.get()`` with a short
        timeout so we can still react to ``stop_event`` when the queue
        has been idle. On shutdown, any events still on the queue are
        drained in a best-effort final pass so counters reconcile.
        """
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            try:
                self._window_manager.dispatch(event)
                self._counters.processed += 1
            except Exception:
                logger.exception("window_manager.dispatch failed")
            finally:
                self._queue.task_done()
        # Drain any remaining events so shutdown is clean.
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover
                break
            try:
                self._window_manager.dispatch(event)
                self._counters.processed += 1
            except Exception:
                logger.exception("window_manager.dispatch failed during drain")
            finally:
                self._queue.task_done()
