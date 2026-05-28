from __future__ import annotations

import asyncio
import json
import time

from src.optimizations import fsm_parser, precompiled_validator  # noqa: F401 — register
from src.optimizations.registry import factory_for, list_optimizations
from src.settings import Settings


def _lines(n: int = 50) -> list[dict]:
    return [
        {"line": json.dumps({"ts": time.time(), "level": "info", "msg": f"m{i}", "field_0": i})}
        for i in range(n)
    ]


async def _drive(pipeline, records):
    runner = asyncio.create_task(pipeline.run())
    feeder = asyncio.create_task(pipeline.feed(records))
    await feeder
    await runner
    return pipeline.sink


async def test_fsm_parser_pipeline_drains() -> None:
    factory = factory_for("fsm_parser")
    pipeline = factory(Settings())
    sink = await _drive(pipeline, _lines(100))
    assert len(sink) == 100


async def test_precompiled_validator_pipeline_drains() -> None:
    factory = factory_for("precompiled_validator")
    pipeline = factory(Settings())
    sink = await _drive(pipeline, _lines(100))
    assert len(sink) == 100


def test_fsm_parser_parses_sample_line() -> None:
    parser = fsm_parser.FSMParser()
    result = parser.parse('{"ts": 1.5, "level": "info", "msg": "hello"}')
    assert result["ts"] == 1.5
    assert result["level"] == "info"
    assert result["msg"] == "hello"


def test_precompiled_validator_rejects_missing_keys() -> None:
    v = precompiled_validator.PrecompiledValidator()
    assert v.validate({"ts": 1.0, "level": "info", "msg": "x"}) is True
    assert v.validate({"ts": 1.0, "level": "info"}) is False


def test_optimizations_list_contains_new() -> None:
    names = {item["name"] for item in list_optimizations()}
    assert {"fsm_parser", "precompiled_validator"} <= names
