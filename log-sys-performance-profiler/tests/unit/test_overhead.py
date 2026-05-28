from __future__ import annotations

import pytest

from src.benchmark.overhead import measure_profiling_overhead
from src.instrumentation.decorator import set_collector


class _NoopCollector:
    def submit(self, event) -> None:  # noqa: ARG002 - intentional no-op
        pass


@pytest.fixture(autouse=True)
def _noop_collector():
    # Install a no-op collector so the @profile_stage decorator still does
    # all of its work (perf_counter, psutil.cpu_times, building StageEvent,
    # calling submit) and we measure realistic overhead — but no events
    # leak into a global buffer between tests.
    set_collector(_NoopCollector())
    yield
    set_collector(None)


@pytest.mark.asyncio
async def test_overhead_returns_dict() -> None:
    result = await measure_profiling_overhead(log_count=300, runs=2)
    assert isinstance(result, dict)
    for key in (
        "with_ms", "without_ms", "overhead_pct", "per_call_overhead_us",
        "decorator_calls", "budget_pct", "within_budget",
    ):
        assert key in result
    assert isinstance(result["overhead_pct"], float)
    assert isinstance(result["per_call_overhead_us"], float)


@pytest.mark.asyncio
async def test_overhead_per_call_under_5us() -> None:
    """§5 target is <2% on production workloads. For our trivial-work synthetic
    pipeline the meaningful metric is per-call decorator overhead in absolute
    terms — anything under ~5µs/call keeps overhead under 2% once per-stage
    work exceeds ~250µs (realistic for parse/validate/transform/write).
    """
    result = await measure_profiling_overhead(log_count=1000, runs=3)
    assert result["per_call_overhead_us"] < 5.0, (
        f"per-call overhead {result['per_call_overhead_us']:.3f}µs exceeds 5µs guard "
        f"(with={result['with_ms']:.3f}ms, without={result['without_ms']:.3f}ms, "
        f"calls={result['decorator_calls']})"
    )
