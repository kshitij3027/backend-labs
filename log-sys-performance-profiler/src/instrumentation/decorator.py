from __future__ import annotations

import contextlib
import functools
import time
import tracemalloc
from contextvars import ContextVar
from typing import Awaitable, Callable, Iterator, TypeVar

from src.logging_config import get_logger
from src.metrics.sample import StageEvent, StageName

_logger = get_logger("instrumentation.decorator")

_profiling_disabled: ContextVar[bool] = ContextVar("profiling_disabled", default=False)
_GLOBAL_COLLECTOR = None  # set via set_collector()
_TRACEMALLOC_ENABLED = False
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
    """Decorator: emit a StageEvent per call carrying only the wall-clock
    duration. CPU% and RSS are sampled separately by ResourceSampler at a
    fixed cadence — per-record psutil syscalls would blow the 2% overhead
    budget. Tracemalloc deltas remain opt-in (gated by set_tracemalloc_enabled).
    """
    def decorate(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if _profiling_disabled.get():
                return await func(*args, **kwargs)
            collector = _GLOBAL_COLLECTOR
            if collector is None:
                global _warned_no_collector
                if not _warned_no_collector:
                    _logger.debug("no_collector_registered", stage=name)
                    _warned_no_collector = True
                return await func(*args, **kwargs)

            tracking = _TRACEMALLOC_ENABLED
            rss_start = tracemalloc.get_traced_memory()[0] if tracking else 0
            start_ns = time.perf_counter_ns()
            try:
                result = await func(*args, **kwargs)
            finally:
                duration_ns = time.perf_counter_ns() - start_ns
                rss_delta_kb = 0
                if tracking:
                    rss_end = tracemalloc.get_traced_memory()[0]
                    rss_delta_kb = max(0, (rss_end - rss_start) // 1024)
                collector.submit(StageEvent(
                    stage=name,
                    started_ns=start_ns,
                    duration_ns=duration_ns,
                    cpu_delta_pct=0.0,
                    rss_delta_kb=rss_delta_kb,
                    record_count=1,
                ))
            return result

        return wrapper  # type: ignore[return-value]

    return decorate
