from __future__ import annotations

import time
from contextlib import AbstractAsyncContextManager

from src.instrumentation.decorator import _GLOBAL_COLLECTOR, _profiling_disabled
from src.metrics.sample import StageEvent, StageName


class StageTimer(AbstractAsyncContextManager):
    """Manual scope timer - alternative to @profile_stage when the decorator
    can't reach (e.g., async generators or per-batch scoping)."""

    def __init__(self, name: StageName, record_count: int = 1) -> None:
        self._name = name
        self._record_count = record_count
        self._start_ns = 0

    async def __aenter__(self) -> "StageTimer":
        self._start_ns = time.perf_counter_ns()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if _profiling_disabled.get():
            return
        from src.instrumentation import decorator as _dec
        collector = _dec._GLOBAL_COLLECTOR
        if collector is None:
            return
        duration_ns = time.perf_counter_ns() - self._start_ns
        event = StageEvent(
            stage=self._name,
            started_ns=self._start_ns,
            duration_ns=duration_ns,
            cpu_delta_pct=0.0,
            rss_delta_kb=0,
            record_count=self._record_count,
        )
        collector.submit(event)
