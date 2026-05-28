from __future__ import annotations

import json

import pytest

from src.instrumentation import decorator as _dec
from src.instrumentation.decorator import set_collector, set_tracemalloc_enabled
from src.loadgen.generator import SyntheticLogGenerator, WorkloadSpec
from src.loadgen.runner import LoadRunner, RunSummary
from src.metrics.ring_buffer import RingBuffer
from src.settings import Settings


@pytest.fixture(autouse=True)
def _reset_decorator_collector():
    # WHY: the @profile_stage decorator references a module-global collector;
    # for these loadgen tests we want it to be a no-op so the pipeline runs
    # without depending on a real MetricsCollector.
    set_collector(None)
    _dec._warned_no_collector = False
    set_tracemalloc_enabled(False)
    yield
    set_collector(None)
    _dec._warned_no_collector = False
    set_tracemalloc_enabled(False)


def test_generator_deterministic() -> None:
    a = list(SyntheticLogGenerator(42).generate(WorkloadSpec(count=10)))
    b = list(SyntheticLogGenerator(42).generate(WorkloadSpec(count=10)))
    # ts uses time.time() so it will differ across calls; strip ts before compare.
    def _strip(records: list[dict]) -> list[dict]:
        stripped = []
        for r in records:
            payload = json.loads(r["line"])
            payload.pop("ts", None)
            stripped.append(payload)
        return stripped

    assert _strip(a) == _strip(b)


def test_generator_default_spec() -> None:
    records = list(SyntheticLogGenerator(7).generate(WorkloadSpec(count=100)))
    assert len(records) == 100
    for r in records:
        assert "line" in r
        payload = json.loads(r["line"])
        assert "ts" in payload
        assert "level" in payload
        assert "msg" in payload
        assert payload["level"] in {"info", "warn", "error"}


@pytest.mark.asyncio
async def test_load_runner_returns_summary() -> None:
    buffer = RingBuffer(1000)
    runner = LoadRunner(buffer, sampler=None, settings=Settings())
    summary = await runner.run(log_count=50, concurrency=2, seed=1)
    assert isinstance(summary, RunSummary)
    # run_id should be a hex string
    assert isinstance(summary.run_id, str)
    assert len(summary.run_id) == 32
    int(summary.run_id, 16)  # raises if not hex
    assert summary.log_count == 50
    assert summary.concurrency == 2
    assert summary.workload_seed == 1
    assert summary.throughput_lps > 0
    assert summary.baseline_or_optimized == "baseline"


@pytest.mark.asyncio
async def test_throughput_positive_on_small_load() -> None:
    buffer = RingBuffer(1000)
    runner = LoadRunner(buffer, sampler=None, settings=Settings())
    summary = await runner.run(log_count=100, concurrency=2, seed=2)
    assert summary.throughput_lps > 0
    assert summary.log_count == 100


@pytest.mark.asyncio
async def test_peak_cpu_and_mem_numeric() -> None:
    buffer = RingBuffer(1000)
    runner = LoadRunner(buffer, sampler=None, settings=Settings())
    summary = await runner.run(log_count=50, concurrency=2, seed=3)
    assert isinstance(summary.peak_cpu, float)
    assert isinstance(summary.peak_mem_mb, float)
    # With no sampler attached and no real collector wiring, samples are empty
    # so peaks default to 0.0. That's expected.
    assert summary.peak_cpu >= 0.0
    assert summary.peak_mem_mb >= 0.0
