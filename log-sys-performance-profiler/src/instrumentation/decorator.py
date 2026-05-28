from __future__ import annotations

import contextlib
import functools
import os
import time
import tracemalloc
from contextvars import ContextVar
from typing import Awaitable, Callable, Iterator, TypeVar

import psutil

from src.logging_config import get_logger
from src.metrics.sample import StageEvent, StageName

_logger = get_logger("instrumentation.decorator")

_profiling_disabled: ContextVar[bool] = ContextVar("profiling_disabled", default=False)
_GLOBAL_COLLECTOR = None  # set via set_collector()
_TRACEMALLOC_ENABLED = False
_PROC = psutil.Process(os.getpid())
_warned_no_collector = False


def set_collector(collector) -> None:
    global _GLOBAL_COLLECTOR
    _GLOBAL_COLLECTOR = collector


def set_tracemalloc_enabled(enabled: bool) -> None:
    global _TRACEMALLOC_ENABLED
    if enabled and not tracemalloc.is_tracing():
        tracemalloc.start()
    elif not enabled and tracemalloc.is_tracing():
        tracemalloc.stop()
    _TRACEMALLOC_ENABLED = enabled


@contextlib.contextmanager
def profiling_disabled() -> Iterator[None]:
    token = _profiling_disabled.set(True)
    try:
        yield
    finally:
        _profiling_disabled.reset(token)


F = TypeVar("F", bound=Callable[..., Awaitable])


def profile_stage(name: StageName) -> Callable[[F], F]:
    def decorate(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if _profiling_disabled.get():
                return await func(*args, **kwargs)
            global _warned_no_collector
            if _GLOBAL_COLLECTOR is None:
                if not _warned_no_collector:
                    _logger.debug("no_collector_registered", stage=name)
                    _warned_no_collector = True
                return await func(*args, **kwargs)

            start_ns = time.perf_counter_ns()
            try:
                cpu_start = _PROC.cpu_times()
                cpu_t0 = cpu_start.user + cpu_start.system
            except psutil.Error:
                cpu_t0 = 0.0
            rss_start = 0
            if _TRACEMALLOC_ENABLED:
                rss_start, _ = tracemalloc.get_traced_memory()

            try:
                result = await func(*args, **kwargs)
            finally:
                duration_ns = time.perf_counter_ns() - start_ns
                try:
                    cpu_end = _PROC.cpu_times()
                    cpu_t1 = cpu_end.user + cpu_end.system
                except psutil.Error:
                    cpu_t1 = cpu_t0
                rss_end = 0
                if _TRACEMALLOC_ENABLED:
                    rss_end, _ = tracemalloc.get_traced_memory()

                wall_s = max(duration_ns / 1_000_000_000.0, 1e-9)
                cpu_delta_pct = max(0.0, (cpu_t1 - cpu_t0) / wall_s * 100.0)
                rss_delta_kb = max(0, (rss_end - rss_start) // 1024)

                event = StageEvent(
                    stage=name,
                    started_ns=start_ns,
                    duration_ns=duration_ns,
                    cpu_delta_pct=cpu_delta_pct,
                    rss_delta_kb=rss_delta_kb,
                    record_count=1,
                )
                _GLOBAL_COLLECTOR.submit(event)
            return result

        return wrapper  # type: ignore[return-value]

    return decorate
