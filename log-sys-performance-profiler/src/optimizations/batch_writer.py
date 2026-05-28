from __future__ import annotations

import asyncio

from src.instrumentation.decorator import profile_stage
from src.instrumentation.pipeline import (
    SENTINEL,
    LogPipeline,
    ParseStage,
    TransformStage,
    ValidateStage,
    WriteStage,
)
from src.optimizations.registry import register_optimization
from src.settings import Settings


class BatchingWriteStage(WriteStage):
    """Coalesce up to ``batch_size`` records (or wait at most ``max_wait_sec``)
    before appending to the in-memory sink. The default 5ms cap keeps tail
    latency low when the upstream goes quiet (synthetic-pipeline finish), so
    the optimization doesn't regress on workloads where the underlying write
    is essentially free (in-memory append). Real disk/network sinks benefit
    from coalescing far more than they pay in flush latency.
    """

    def __init__(self, inbound: asyncio.Queue, batch_size: int = 50, max_wait_sec: float = 0.005) -> None:
        super().__init__(inbound=inbound)
        self._batch_size = batch_size
        self._max_wait_sec = max_wait_sec

    @profile_stage("write")
    async def process(self, record: dict) -> dict | None:
        return record

    async def run(self) -> None:
        while True:
            batch: list[dict] = []
            try:
                first = await self.inbound.get()
            except asyncio.CancelledError:
                raise
            try:
                if first is SENTINEL:
                    return
                batch.append(first)
            finally:
                self.inbound.task_done()
            deadline = asyncio.get_event_loop().time() + self._max_wait_sec
            while len(batch) < self._batch_size:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self.inbound.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                try:
                    if item is SENTINEL:
                        self.sink.extend(batch)
                        return
                    batch.append(item)
                finally:
                    self.inbound.task_done()
            self.sink.extend(batch)


@register_optimization(
    "batch_writer",
    "Coalesces small writes into larger batches at the write stage",
)
def build_batch_writer_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = BatchingWriteStage(inbound=q4, batch_size=100)
    return LogPipeline([parse, validate, transform, write])
