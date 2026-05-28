from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path

import pytest

from src.optimizations import async_io_variant, mmap_reader  # noqa: F401 - register
from src.optimizations.registry import factory_for, list_optimizations
from src.settings import Settings


def _lines(n: int = 50) -> list[dict]:
    return [
        {"line": json.dumps({"ts": time.time(), "level": "info", "msg": f"m{i}"})}
        for i in range(n)
    ]


async def _drive(pipeline, records):
    runner = asyncio.create_task(pipeline.run())
    feeder = asyncio.create_task(pipeline.feed(records))
    await feeder
    await runner
    return pipeline.sink


async def test_async_io_variant_drains_and_writes_file(tmp_path: Path) -> None:
    out = tmp_path / "async_sink.jsonl"
    stage = async_io_variant.AsyncFileWriteStage(
        inbound=asyncio.Queue(maxsize=64), path=out
    )
    # Build a custom pipeline since the factory uses a fixed default path.
    from src.instrumentation.pipeline import (
        LogPipeline, ParseStage, ValidateStage, TransformStage,
    )
    qmax = 64
    q1 = asyncio.Queue(maxsize=qmax)
    q2 = asyncio.Queue(maxsize=qmax)
    q3 = asyncio.Queue(maxsize=qmax)
    q4 = stage.inbound
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    pipeline = LogPipeline([parse, validate, transform, stage])

    sink = await _drive(pipeline, _lines(50))
    assert len(sink) == 50
    assert out.exists()
    assert out.stat().st_size > 0


async def test_factory_for_async_io_variant_returns_callable() -> None:
    factory = factory_for("async_io_variant")
    assert callable(factory)


async def test_factory_for_mmap_reader_returns_callable() -> None:
    factory = factory_for("mmap_reader")
    pipeline = factory(Settings())
    sink = await _drive(pipeline, _lines(20))
    assert len(sink) == 20


def test_mmap_reader_yields_lines_from_tempfile(tmp_path: Path) -> None:
    p = tmp_path / "input.jsonl"
    with open(p, "w") as f:
        for i in range(5):
            f.write(json.dumps({"ts": time.time(), "level": "info", "msg": f"x{i}"}) + "\n")
    reader = mmap_reader.MmapLogReader(p)
    items = list(reader)
    assert len(items) == 5
    assert all("line" in i for i in items)


def test_optimizations_list_contains_io() -> None:
    names = {item["name"] for item in list_optimizations()}
    assert {"async_io_variant", "mmap_reader"} <= names
