from __future__ import annotations

import asyncio
import json
import time

import pytest

from src.instrumentation.pipeline import (
    LogPipeline,
    ParseStage,
    TransformStage,
    ValidateStage,
    WriteStage,
    build_default_pipeline,
)
from src.settings import Settings


def _make_line(level: str = "info", msg: str = "x") -> dict:
    return {"line": json.dumps({"ts": time.time(), "level": level, "msg": msg})}


async def _run_with(pipeline: LogPipeline, records: list[dict]) -> None:
    feeder = asyncio.create_task(pipeline.feed(records))
    runner = asyncio.create_task(pipeline.run())
    await feeder
    await runner


@pytest.mark.asyncio
async def test_pipeline_drains_records() -> None:
    settings = Settings()
    pipeline = build_default_pipeline(settings)
    records = [_make_line(msg=f"m{i}") for i in range(100)]
    await _run_with(pipeline, records)
    assert len(pipeline.sink) == 100
    for r in pipeline.sink:
        assert "host" in r
        assert "env" in r


@pytest.mark.asyncio
async def test_validate_drops_malformed() -> None:
    settings = Settings()
    pipeline = build_default_pipeline(settings)
    good = [_make_line(msg=f"ok{i}") for i in range(5)]
    bad = [{"line": json.dumps({"level": "info", "msg": "no-ts"})} for _ in range(5)]
    await _run_with(pipeline, good + bad)
    assert len(pipeline.sink) == 5


@pytest.mark.asyncio
async def test_transform_adds_host_env() -> None:
    settings = Settings()
    pipeline = build_default_pipeline(settings)
    await _run_with(pipeline, [_make_line(msg="solo")])
    assert len(pipeline.sink) == 1
    record = pipeline.sink[0]
    assert "host" in record
    assert "env" in record
    assert record["env"] == "dev"


@pytest.mark.asyncio
async def test_write_stage_collects_records() -> None:
    settings = Settings()
    pipeline = build_default_pipeline(settings)
    records = [_make_line(msg=f"r{i}") for i in range(5)]
    await _run_with(pipeline, records)
    assert len(pipeline.sink) == 5


@pytest.mark.asyncio
async def test_queue_depth_for_returns_int() -> None:
    settings = Settings()
    pipeline = build_default_pipeline(settings)
    depth = pipeline.queue_depth_for("parse")
    assert isinstance(depth, int)
    assert depth >= 0


@pytest.mark.asyncio
async def test_exception_in_stage_does_not_kill_pipeline() -> None:
    settings = Settings()

    class FlakyTransform(TransformStage):
        def __init__(self, inbound: asyncio.Queue, outbound: asyncio.Queue | None) -> None:
            super().__init__(inbound, outbound)
            self._count = 0

        async def process(self, record: dict) -> dict | None:
            self._count += 1
            if self._count % 2 == 0:
                raise RuntimeError("synthetic flake")
            return await super().process(record)

    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = FlakyTransform(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    pipeline = LogPipeline([parse, validate, transform, write])

    records = [_make_line(msg=f"r{i}") for i in range(10)]
    await asyncio.wait_for(_run_with(pipeline, records), timeout=5.0)

    # Every other record (5 of 10) was dropped by the flake; survivors collected.
    assert 3 <= len(pipeline.sink) <= 7
