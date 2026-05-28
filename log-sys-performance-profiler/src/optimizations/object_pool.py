from __future__ import annotations

import asyncio
from collections import deque
from typing import Callable, Generic, TypeVar

from src.instrumentation.decorator import profile_stage
from src.instrumentation.pipeline import (
    LogPipeline,
    ParseStage,
    TransformStage,
    ValidateStage,
    WriteStage,
)
from src.optimizations.registry import register_optimization
from src.settings import Settings

T = TypeVar("T")


class ObjectPool(Generic[T]):
    """Simple bounded recycling pool. Caller must release() after use."""

    def __init__(self, factory: Callable[[], T], max_size: int = 256) -> None:
        self._factory = factory
        self._max_size = max_size
        self._pool: deque[T] = deque()
        self._created = 0
        self._reused = 0

    def acquire(self) -> T:
        if self._pool:
            self._reused += 1
            return self._pool.popleft()
        self._created += 1
        return self._factory()

    def release(self, obj: T) -> None:
        if len(self._pool) < self._max_size:
            if isinstance(obj, dict):
                obj.clear()
            self._pool.append(obj)

    @property
    def stats(self) -> dict[str, int]:
        return {"created": self._created, "reused": self._reused, "in_pool": len(self._pool)}


class PooledTransformStage(TransformStage):
    """Transform stage that recycles output dicts via an ObjectPool, lowering
    GC pressure under high-allocation workloads."""

    def __init__(
        self,
        inbound: asyncio.Queue,
        outbound: asyncio.Queue | None,
        host: str | None = None,
        env: str = "dev",
        pool_size: int = 256,
    ) -> None:
        super().__init__(inbound=inbound, outbound=outbound, host=host, env=env)
        self._pool: ObjectPool[dict] = ObjectPool(dict, max_size=pool_size)

    @profile_stage("transform")
    async def process(self, record: dict) -> dict | None:
        out = self._pool.acquire()
        out.update(record)
        out["host"] = self._host
        out["env"] = self._env
        return out


@register_optimization(
    "object_pool",
    "Recycles per-record dicts in the transform stage to reduce GC pressure",
)
def build_object_pool_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = PooledTransformStage(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
