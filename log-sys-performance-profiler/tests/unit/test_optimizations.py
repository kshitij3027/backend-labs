from __future__ import annotations

import asyncio
import json
import time

import pytest

from src.optimizations import batch_writer, object_pool  # noqa: F401 — register
from src.optimizations.registry import factory_for, list_optimizations
from src.settings import Settings


def _line(n: int = 1) -> list[dict]:
    return [
        {"line": json.dumps({"ts": time.time(), "level": "info", "msg": f"m{i}"})}
        for i in range(n)
    ]


async def _drive(pipeline, records: list[dict]) -> list[dict]:
    runner = asyncio.create_task(pipeline.run())
    feeder = asyncio.create_task(pipeline.feed(records))
    await feeder
    await runner
    return pipeline.sink


async def test_factory_for_batch_writer_returns_callable() -> None:
    factory = factory_for("batch_writer")
    pipeline = factory(Settings())
    sink = await _drive(pipeline, _line(100))
    assert len(sink) == 100


async def test_factory_for_object_pool_returns_callable() -> None:
    factory = factory_for("object_pool")
    pipeline = factory(Settings())
    sink = await _drive(pipeline, _line(50))
    assert len(sink) == 50
    assert all("host" in r and "env" in r for r in sink)


async def test_unknown_optimization_raises() -> None:
    with pytest.raises(KeyError):
        factory_for("nonexistent")


def test_list_optimizations_has_two() -> None:
    names = {item["name"] for item in list_optimizations()}
    assert {"batch_writer", "object_pool"} <= names


async def test_object_pool_recycles() -> None:
    pool = object_pool.ObjectPool(dict, max_size=4)
    a = pool.acquire()
    pool.release(a)
    b = pool.acquire()
    assert pool.stats["reused"] >= 1
