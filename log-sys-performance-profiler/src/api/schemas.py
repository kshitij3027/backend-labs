from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class StartRunRequest(BaseModel):
    log_count: int = Field(default=1000, ge=1, le=1_000_000)
    concurrency: int = Field(default=4, ge=1, le=64)
    seed: int = 42
    optimization_name: Optional[str] = None


class StartRunResponse(BaseModel):
    run_id: str
    mode: Literal["baseline", "compare"]
    baseline_run_id: Optional[str] = None
    optimized_run_id: Optional[str] = None


class RunSummaryOut(BaseModel):
    run_id: str
    started_at: float
    finished_at: float
    baseline_or_optimized: str
    optimization_name: Optional[str]
    workload_seed: int
    log_count: int
    concurrency: int
    throughput_lps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    peak_cpu: float
    peak_mem_mb: float
    bottlenecks: list[dict] = []
    recommendations: list[dict] = []
    samples: list[dict] = []
