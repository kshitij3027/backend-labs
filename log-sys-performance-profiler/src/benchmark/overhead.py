from __future__ import annotations

import asyncio
import statistics
import time

from src.instrumentation.decorator import profiling_disabled
from src.instrumentation.pipeline import build_default_pipeline
from src.loadgen.generator import SyntheticLogGenerator, WorkloadSpec
from src.logging_config import get_logger
from src.settings import get_settings

_logger = get_logger("benchmark.overhead")


async def _run_once(log_count: int, seed: int) -> float:
    settings = get_settings()
    pipeline = build_default_pipeline(settings)
    records = list(SyntheticLogGenerator(seed).generate(WorkloadSpec(count=log_count)))
    t0 = time.perf_counter()
    runner = asyncio.create_task(pipeline.run())
    feeder = asyncio.create_task(pipeline.feed(records))
    await feeder
    await runner
    return time.perf_counter() - t0


async def measure_profiling_overhead(
    log_count: int = 2000, seed: int = 7, runs: int = 3
) -> dict:
    """Run the pipeline `runs` times with profiling ENABLED and `runs` times
    with it DISABLED, take medians, and report both percent overhead and the
    per-call absolute overhead. The §5 <2% target applies to production-scale
    workloads; the per-call (microsecond) metric is the meaningful gauge for
    trivial-work synthetic pipelines.
    """
    settings = get_settings()
    await _run_once(log_count=min(log_count, 500), seed=seed)

    without: list[float] = []
    for _ in range(runs):
        with profiling_disabled():
            without.append(await _run_once(log_count, seed))

    with_on: list[float] = []
    for _ in range(runs):
        with_on.append(await _run_once(log_count, seed))

    med_without = statistics.median(without)
    med_with = statistics.median(with_on)
    overhead_pct = (
        max(0.0, (med_with - med_without) / med_without * 100.0)
        if med_without > 0 else 0.0
    )
    decorator_calls = log_count * len(settings.instrumented_stages_list)
    per_call_overhead_us = (
        max(0.0, (med_with - med_without) / decorator_calls * 1_000_000.0)
        if decorator_calls > 0 else 0.0
    )
    budget = float(settings.overhead_target_pct)
    within = overhead_pct <= budget

    if not within:
        _logger.warning(
            "profiling_overhead_exceeded",
            overhead_pct=overhead_pct,
            per_call_overhead_us=per_call_overhead_us,
            budget_pct=budget,
        )

    return {
        "with_ms": med_with * 1000.0,
        "without_ms": med_without * 1000.0,
        "with_pct": med_with * 1000.0,
        "without_pct": med_without * 1000.0,
        "overhead_pct": overhead_pct,
        "per_call_overhead_us": per_call_overhead_us,
        "decorator_calls": decorator_calls,
        "budget_pct": budget,
        "within_budget": within,
    }
