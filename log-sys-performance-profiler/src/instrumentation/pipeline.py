from __future__ import annotations

import asyncio
import json
import socket
from typing import Any, Iterable, Protocol, runtime_checkable

from src.instrumentation.decorator import profile_stage
from src.logging_config import get_logger
from src.metrics.sample import StageName
from src.settings import Settings

_logger = get_logger("pipeline")


class _Sentinel:
    """Singleton end-of-stream marker."""
    pass


SENTINEL: Any = _Sentinel()


@runtime_checkable
class Stage(Protocol):
    name: StageName
    inbound: asyncio.Queue
    outbound: asyncio.Queue | None

    async def process(self, record: dict) -> dict | None: ...
    async def run(self) -> None: ...


class _BaseStage:
    """Concrete shared run() loop. Subclasses override process()."""

    name: StageName

    def __init__(self, inbound: asyncio.Queue, outbound: asyncio.Queue | None) -> None:
        self.inbound = inbound
        self.outbound = outbound

    async def process(self, record: dict) -> dict | None:
        raise NotImplementedError

    async def run(self) -> None:
        while True:
            item = await self.inbound.get()
            try:
                if item is SENTINEL:
                    if self.outbound is not None:
                        await self.outbound.put(SENTINEL)
                    return
                try:
                    result = await self.process(item)
                except Exception as exc:
                    _logger.warning(
                        "stage_error", stage=self.name, error=str(exc)
                    )
                    continue
                if result is None:
                    continue
                if self.outbound is not None:
                    await self.outbound.put(result)
                else:
                    # Write stage: forwarded to internal sink list.
                    sink = getattr(self, "sink", None)
                    if sink is not None:
                        sink.append(result)
            finally:
                self.inbound.task_done()


class ParseStage(_BaseStage):
    name: StageName = "parse"

    @profile_stage("parse")
    async def process(self, record: dict) -> dict | None:
        line = record.get("line", "")
        if not line:
            return None
        try:
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                return {"raw": line, "level": "info"}
            return parsed
        except (json.JSONDecodeError, TypeError):
            return {"raw": line, "level": "info"}


class ValidateStage(_BaseStage):
    name: StageName = "validate"
    _REQUIRED = frozenset({"ts", "level", "msg"})

    @profile_stage("validate")
    async def process(self, record: dict) -> dict | None:
        if not self._REQUIRED.issubset(record.keys()):
            return None
        return record


class TransformStage(_BaseStage):
    name: StageName = "transform"

    def __init__(
        self,
        inbound: asyncio.Queue,
        outbound: asyncio.Queue | None,
        host: str | None = None,
        env: str = "dev",
    ) -> None:
        super().__init__(inbound, outbound)
        self._host = host or socket.gethostname()
        self._env = env

    @profile_stage("transform")
    async def process(self, record: dict) -> dict | None:
        record = dict(record)
        record["host"] = self._host
        record["env"] = self._env
        return record


class WriteStage(_BaseStage):
    name: StageName = "write"

    def __init__(self, inbound: asyncio.Queue) -> None:
        super().__init__(inbound, outbound=None)
        self.sink: list[dict] = []

    @profile_stage("write")
    async def process(self, record: dict) -> dict | None:
        return record


class LogPipeline:
    """Wires stages with bounded queues; exposes feed() and run()."""

    def __init__(self, stages: list[_BaseStage]) -> None:
        if len(stages) < 1:
            raise ValueError("pipeline requires at least one stage")
        self._stages = stages
        self._first = stages[0]
        self._write = stages[-1]

    @property
    def stages(self) -> list[_BaseStage]:
        return self._stages

    @property
    def sink(self) -> list[dict]:
        return getattr(self._write, "sink", [])

    def queue_depth_for(self, stage_name: StageName) -> int:
        for s in self._stages:
            if s.name == stage_name:
                return s.inbound.qsize()
        return 0

    async def feed(self, records: Iterable[dict]) -> None:
        for r in records:
            await self._first.inbound.put(r)
        await self._first.inbound.put(SENTINEL)

    async def run(self) -> None:
        await asyncio.gather(*(s.run() for s in self._stages))


def build_default_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
