from __future__ import annotations

import asyncio
import json
from pathlib import Path

import aiofiles

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

_DEFAULT_PATH = Path("/app/logs/async_sink.jsonl")


class AsyncFileWriteStage(WriteStage):
    """Async-IO write stage backed by aiofiles. Overlaps file write waits
    with other coroutines' CPU work.

    Lazy-opens the file on first record. Falls back to the in-memory sink
    if the path is not writable (e.g., read-only filesystem during tests).
    """

    def __init__(self, inbound: asyncio.Queue, path: Path | None = None) -> None:
        super().__init__(inbound=inbound)
        self._path = path or _DEFAULT_PATH
        self._handle = None
        self._fallback = False

    @profile_stage("write")
    async def process(self, record: dict) -> dict | None:
        if self._fallback:
            return record
        if self._handle is None:
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._handle = await aiofiles.open(self._path, "a")
            except OSError:
                self._fallback = True
                return record
        try:
            await self._handle.write(json.dumps(record) + "\n")
        except OSError:
            self._fallback = True
        return record

    async def run(self) -> None:
        try:
            while True:
                item = await self.inbound.get()
                try:
                    if item is SENTINEL:
                        return
                    try:
                        result = await self.process(item)
                    except Exception:
                        result = None
                    if result is not None:
                        self.sink.append(result)
                finally:
                    self.inbound.task_done()
        finally:
            if self._handle is not None:
                try:
                    await self._handle.flush()
                    await self._handle.close()
                except Exception:
                    pass


@register_optimization(
    "async_io_variant",
    "Async-IO file writes - overlaps disk waits with CPU work in the write stage",
)
def build_async_io_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = AsyncFileWriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
