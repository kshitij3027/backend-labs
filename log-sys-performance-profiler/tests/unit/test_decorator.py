from __future__ import annotations

import asyncio

import pytest

from src.instrumentation import decorator as _dec
from src.instrumentation.decorator import (
    profile_stage,
    profiling_disabled,
    set_collector,
    set_tracemalloc_enabled,
)


class _StubCollector:
    def __init__(self):
        self.events = []

    def submit(self, event):
        self.events.append(event)


@pytest.fixture(autouse=True)
def reset_collector():
    # WHY: keep tests isolated — the decorator stores the collector on the
    # module, so tests must wipe it before/after each run.
    set_collector(None)
    _dec._warned_no_collector = False
    set_tracemalloc_enabled(False)
    yield
    set_collector(None)
    _dec._warned_no_collector = False
    set_tracemalloc_enabled(False)


@pytest.mark.asyncio
async def test_decorator_emits_one_event_per_call():
    stub = _StubCollector()
    set_collector(stub)

    @profile_stage("parse")
    async def f(x):
        return x * 2

    result = await f(3)

    assert result == 6
    assert len(stub.events) == 1
    assert stub.events[0].stage == "parse"


@pytest.mark.asyncio
async def test_profiling_disabled_skips_emission():
    stub = _StubCollector()
    set_collector(stub)

    @profile_stage("parse")
    async def f(x):
        return x * 2

    with profiling_disabled():
        result = await f(3)

    assert result == 6
    assert stub.events == []


@pytest.mark.asyncio
async def test_decorator_propagates_exception():
    stub = _StubCollector()
    set_collector(stub)

    @profile_stage("parse")
    async def boom():
        raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        await boom()

    # finally-block must still emit the event even on exception
    assert len(stub.events) == 1
    assert stub.events[0].stage == "parse"


@pytest.mark.asyncio
async def test_event_fields_populated():
    stub = _StubCollector()
    set_collector(stub)

    @profile_stage("validate")
    async def f():
        return None

    await f()

    assert len(stub.events) == 1
    evt = stub.events[0]
    assert evt.duration_ns > 0
    assert evt.started_ns > 0
    assert evt.record_count == 1


@pytest.mark.asyncio
async def test_no_collector_makes_decorator_noop():
    set_collector(None)

    @profile_stage("parse")
    async def f(x):
        return x + 1

    # should not raise even without a collector
    result = await f(1)
    assert result == 2


@pytest.mark.asyncio
async def test_tracemalloc_gate_respected_when_false():
    stub = _StubCollector()
    set_collector(stub)
    set_tracemalloc_enabled(False)

    @profile_stage("transform")
    async def f():
        return [i for i in range(100)]

    await f()

    assert len(stub.events) == 1
    assert stub.events[0].rss_delta_kb == 0


@pytest.mark.asyncio
async def test_perf_measurement_positive():
    stub = _StubCollector()
    set_collector(stub)

    @profile_stage("write")
    async def f():
        await asyncio.sleep(0.01)

    await f()

    assert len(stub.events) == 1
    # 0.01s = 10_000_000 ns; allow generous slack for slow CI
    assert stub.events[0].duration_ns >= 5_000_000
