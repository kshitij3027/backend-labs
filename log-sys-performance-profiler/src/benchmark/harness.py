from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from src.analysis.detector import BottleneckDetector
from src.analysis.recommender import RecommendationEngine
from src.instrumentation.pipeline import build_default_pipeline
from src.loadgen.runner import LoadRunner, RunSummary
from src.optimizations.registry import factory_for
from src.settings import Settings
from src.store.run_store import RunStore


Verdict = Literal["improved", "regressed", "neutral"]


@dataclass(slots=True, frozen=True)
class DiffReport:
    baseline_run_id: str
    optimized_run_id: str
    optimization_name: str
    throughput_delta_pct: float
    p95_delta_pct: float
    p99_delta_pct: float
    peak_cpu_delta_pct: float
    peak_mem_delta_pct: float
    verdict: Verdict

    def to_dict(self) -> dict:
        return asdict(self)


def _pct_delta(a: float, b: float) -> float:
    """Percent change from a to b. If a is 0, returns 0 (avoid div/0)."""
    if a <= 0:
        return 0.0
    return (b - a) / a * 100.0


def diff_summaries(baseline: RunSummary, optimized: RunSummary, optimization_name: str) -> DiffReport:
    throughput_delta = _pct_delta(baseline.throughput_lps, optimized.throughput_lps)
    p95_delta = _pct_delta(baseline.p95_ms, optimized.p95_ms)
    p99_delta = _pct_delta(baseline.p99_ms, optimized.p99_ms)
    peak_cpu_delta = _pct_delta(baseline.peak_cpu, optimized.peak_cpu)
    peak_mem_delta = _pct_delta(baseline.peak_mem_mb, optimized.peak_mem_mb)

    # Verdict
    big_wins = throughput_delta >= 10.0 or p95_delta <= -10.0
    big_losses = (
        throughput_delta <= -10.0
        or p95_delta >= 10.0
        or peak_cpu_delta >= 10.0
        or peak_mem_delta >= 10.0
    )
    if big_wins and not big_losses:
        verdict: Verdict = "improved"
    elif big_losses and not big_wins:
        verdict = "regressed"
    else:
        verdict = "neutral"

    return DiffReport(
        baseline_run_id=baseline.run_id,
        optimized_run_id=optimized.run_id,
        optimization_name=optimization_name,
        throughput_delta_pct=throughput_delta,
        p95_delta_pct=p95_delta,
        p99_delta_pct=p99_delta,
        peak_cpu_delta_pct=peak_cpu_delta,
        peak_mem_delta_pct=peak_mem_delta,
        verdict=verdict,
    )


class BeforeAfterHarness:
    def __init__(
        self,
        runner: LoadRunner,
        store: RunStore,
        settings: Settings,
    ) -> None:
        self._runner = runner
        self._store = store
        self._settings = settings
        self._detector = BottleneckDetector(runner._buffer, settings)
        self._recommender = RecommendationEngine()

    async def compare(
        self,
        optimization_name: str,
        log_count: int = 1000,
        concurrency: int = 4,
        seed: int = 42,
    ) -> DiffReport:
        if not optimization_name:
            raise ValueError("optimization_name is required")
        opt_factory = factory_for(optimization_name)  # raises KeyError on unknown

        baseline = await self._run_with(
            label="baseline",
            pipeline_factory=build_default_pipeline,
            log_count=log_count,
            concurrency=concurrency,
            seed=seed,
            optimization_name=None,
        )
        optimized = await self._run_with(
            label="optimized",
            pipeline_factory=opt_factory,
            log_count=log_count,
            concurrency=concurrency,
            seed=seed,
            optimization_name=optimization_name,
        )
        return diff_summaries(baseline, optimized, optimization_name)

    async def _run_with(
        self,
        label,
        pipeline_factory,
        log_count: int,
        concurrency: int,
        seed: int,
        optimization_name,
    ) -> RunSummary:
        summary = await self._runner.run(
            label=label,
            log_count=log_count,
            concurrency=concurrency,
            seed=seed,
            pipeline_factory=pipeline_factory,
            optimization_name=optimization_name,
        )
        # Attach bottlenecks + recommendations using the post-run buffer snapshot
        try:
            bottlenecks = self._detector.evaluate(throughput_lps=summary.throughput_lps)
        except Exception:
            bottlenecks = []
        try:
            recommendations = self._recommender.recommend(bottlenecks)
        except Exception:
            recommendations = []
        summary.bottlenecks = [b.to_dict() for b in bottlenecks]
        summary.recommendations = [r.to_dict() for r in recommendations]
        self._store.put(summary)
        return summary
