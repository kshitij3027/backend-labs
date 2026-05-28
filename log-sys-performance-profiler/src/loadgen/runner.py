from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Callable, Literal

from src.instrumentation.pipeline import LogPipeline, build_default_pipeline
from src.loadgen.generator import SyntheticLogGenerator, WorkloadSpec
from src.metrics.ring_buffer import RingBuffer
from src.resource_sampler.sampler import ResourceSampler
from src.settings import Settings


@dataclass(slots=True)
class RunSummary:
    run_id: str
    started_at: float
    finished_at: float
    baseline_or_optimized: Literal["baseline", "optimized"]
    optimization_name: str | None
    workload_seed: int
    log_count: int
    concurrency: int
    throughput_lps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    peak_cpu: float
    peak_mem_mb: float
    bottlenecks: list[dict] = field(default_factory=list)
    recommendations: list[dict] = field(default_factory=list)
    samples: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


PipelineFactory = Callable[[Settings], LogPipeline]


def _local_percentile(values: list[float], pct: float) -> float:
    """Inline percentile to avoid commit-7 import dependency."""
    if not values:
        return 0.0
    s = sorted(values)
    if pct <= 0:
        return s[0]
    if pct >= 100:
        return s[-1]
    k = (len(s) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


class LoadRunner:
    def __init__(
        self,
        buffer: RingBuffer,
        sampler: ResourceSampler | None,
        settings: Settings,
    ) -> None:
        self._buffer = buffer
        self._sampler = sampler
        self._settings = settings

    async def run(
        self,
        *,
        label: Literal["baseline", "optimized"] = "baseline",
        log_count: int = 1000,
        concurrency: int = 4,
        seed: int = 42,
        pipeline_factory: PipelineFactory | None = None,
        optimization_name: str | None = None,
    ) -> RunSummary:
        run_id = uuid.uuid4().hex
        self._buffer.clear()

        pipeline = (pipeline_factory or build_default_pipeline)(self._settings)
        if self._sampler is not None:
            self._sampler.set_queue_depth_fn(
                lambda stage: pipeline.queue_depth_for(stage)
            )

        records = list(
            SyntheticLogGenerator(seed).generate(WorkloadSpec(count=log_count))
        )

        started_at = time.time()
        runner_task = asyncio.create_task(pipeline.run())
        feeder_task = asyncio.create_task(pipeline.feed(records))
        await feeder_task
        await runner_task
        finished_at = time.time()

        wall = max(finished_at - started_at, 1e-9)
        throughput_lps = log_count / wall

        samples = self._buffer.snapshot()
        latencies = [s.latency_ms for s in samples]
        p50 = _local_percentile(latencies, 50)
        p95 = _local_percentile(latencies, 95)
        p99 = _local_percentile(latencies, 99)
        peak_cpu = max((s.cpu_pct for s in samples), default=0.0)
        peak_mem = max((s.mem_mb for s in samples), default=0.0)

        return RunSummary(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            baseline_or_optimized=label,
            optimization_name=optimization_name,
            workload_seed=seed,
            log_count=log_count,
            concurrency=concurrency,
            throughput_lps=throughput_lps,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99,
            peak_cpu=peak_cpu,
            peak_mem_mb=peak_mem,
            bottlenecks=[],
            recommendations=[],
            samples=[s.to_dict() for s in samples],
        )
