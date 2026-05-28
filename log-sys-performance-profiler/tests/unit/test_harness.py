from __future__ import annotations

import pytest

from src.benchmark.harness import BeforeAfterHarness, diff_summaries
from src.instrumentation.decorator import set_collector
from src.loadgen.runner import LoadRunner, RunSummary
from src.metrics.ring_buffer import RingBuffer
from src.optimizations import batch_writer, object_pool  # noqa: F401 - register
from src.settings import Settings
from src.store.run_store import RunStore


class _NoopCollector:
    def submit(self, event) -> None:  # noqa: ARG002
        pass


@pytest.fixture(autouse=True)
def _noop_collector():
    set_collector(_NoopCollector())
    yield
    set_collector(None)


def _summary(rid, started_at, throughput, p95, peak_cpu, peak_mem) -> RunSummary:
    return RunSummary(
        run_id=rid, started_at=started_at, finished_at=started_at + 1.0,
        baseline_or_optimized="baseline", optimization_name=None,
        workload_seed=42, log_count=100, concurrency=4,
        throughput_lps=throughput, p50_ms=1.0, p95_ms=p95, p99_ms=p95 + 1.0,
        peak_cpu=peak_cpu, peak_mem_mb=peak_mem,
    )


def test_diff_summaries_verdict_improved():
    a = _summary("a", 0.0, throughput=100.0, p95=20.0, peak_cpu=50.0, peak_mem=100.0)
    b = _summary("b", 1.0, throughput=200.0, p95=10.0, peak_cpu=50.0, peak_mem=100.0)
    report = diff_summaries(a, b, "batch_writer")
    assert report.verdict == "improved"
    assert report.throughput_delta_pct == pytest.approx(100.0)
    assert report.p95_delta_pct == pytest.approx(-50.0)


def test_diff_summaries_verdict_regressed():
    a = _summary("a", 0.0, throughput=200.0, p95=10.0, peak_cpu=50.0, peak_mem=100.0)
    b = _summary("b", 1.0, throughput=100.0, p95=20.0, peak_cpu=50.0, peak_mem=100.0)
    report = diff_summaries(a, b, "batch_writer")
    assert report.verdict == "regressed"


def test_diff_summaries_verdict_neutral():
    a = _summary("a", 0.0, throughput=100.0, p95=10.0, peak_cpu=50.0, peak_mem=100.0)
    b = _summary("b", 1.0, throughput=101.0, p95=10.5, peak_cpu=50.5, peak_mem=100.5)
    report = diff_summaries(a, b, "batch_writer")
    assert report.verdict == "neutral"


@pytest.mark.asyncio
async def test_harness_compare_runs_both_pipelines() -> None:
    settings = Settings()
    buffer = RingBuffer(maxlen=settings.metrics_buffer_size)
    runner = LoadRunner(buffer=buffer, sampler=None, settings=settings)
    store = RunStore()
    harness = BeforeAfterHarness(runner=runner, store=store, settings=settings)
    report = await harness.compare(
        optimization_name="batch_writer", log_count=200, concurrency=2, seed=1
    )
    assert report.baseline_run_id != report.optimized_run_id
    assert report.optimization_name == "batch_writer"
    assert report.verdict in {"improved", "regressed", "neutral"}
    assert len(store) == 2


@pytest.mark.asyncio
async def test_harness_unknown_optimization_raises() -> None:
    settings = Settings()
    buffer = RingBuffer(maxlen=settings.metrics_buffer_size)
    runner = LoadRunner(buffer=buffer, sampler=None, settings=settings)
    store = RunStore()
    harness = BeforeAfterHarness(runner=runner, store=store, settings=settings)
    with pytest.raises(KeyError):
        await harness.compare(optimization_name="nonexistent", log_count=10, concurrency=1, seed=1)
