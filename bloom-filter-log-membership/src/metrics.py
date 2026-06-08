"""Per-filter operational metrics — counters and bounded latency windows.

The membership service is judged not just on its answers but on how cheaply
it produces them, so every named filter gets a :class:`FilterMetrics` ledger:
how many adds and queries it served, how the positive/negative split looks,
how many bloom positives the two-tier pipeline (C10) later *disproved*
against authoritative storage (the **observed** false-positive rate, as
opposed to ``bloom.py``'s fill-based *estimate*), and how long operations
actually took. ``GET /stats`` (C8) and the dashboard (C12) read these
numbers straight off :meth:`MetricsRegistry.snapshot`.

Why the latency windows are bounded
-----------------------------------
Latency samples land in a ``collections.deque(maxlen=1000)`` per operation
kind. A deque with ``maxlen`` evicts its oldest entry on every append once
full, so the memory cost of latency tracking is fixed *forever* — after a
million operations or a month of uptime, still exactly 1000 floats per
window. "No memory leaks during extended operation" is a stated success
criterion for this project, and an unbounded sample list is precisely the
slow-burn leak that criterion forbids. The trade: ``p50``/``p99`` describe
the most recent 1000 operations — a rolling operational view, which is what
a live dashboard wants anyway — while ``avg_*_ms`` comes from a running
sum/count pair and therefore covers the filter's whole lifetime in O(1)
space.

Why nearest-rank percentiles
----------------------------
``p50``/``p99`` use the nearest-rank definition: sort a copy of the window
and take the value at index ``ceil(P/100 · N) − 1``. No interpolation — the
reported number is always a latency that actually occurred, the convention
is deterministic and trivially testable (p50 of 1..100 is exactly 50.0, p99
is exactly 99.0), and on a 1000-sample window any difference from the
interpolated definitions is one sample's worth of noise. These figures feed
a human-facing stats endpoint, not an SLO billing system; sorting 1000
floats on demand (microseconds, C-speed) beats maintaining a streaming
quantile sketch we would then have to explain, tune, and test.

Why one ``threading.Lock`` per filter is enough
-----------------------------------------------
Updates arrive from the asyncio event-loop thread (the hot ``async def``
add/query handlers) *and* from AnyIO threadpool worker threads (the sync
pipeline/sessions/demo handlers) — so a real ``threading.Lock`` is
required; an ``asyncio.Lock`` only coordinates coroutines on one thread and
cannot even be awaited from a worker thread. One lock per
:class:`FilterMetrics` then suffices: each critical section is a handful of
int/float bumps plus a deque append — well under a microsecond — while an
uncontended CPython lock acquire/release costs tens of nanoseconds. At this
service's rates (thousands of ops/s across a few threads) the lock is
essentially never contended; sharded counters or lock-free tricks would buy
nothing measurable and cost real complexity. The same reasoning gives
:class:`MetricsRegistry` a single lock around its name → metrics dict,
taken only on lookup and snapshot.
"""
from __future__ import annotations

import math
import threading
from collections import deque

#: Recent latency samples kept per operation kind. 1000 is plenty for a
#: stable p50/p99 reading while pinning each window at ~8 KB of floats no
#: matter how long the service runs (see module docstring).
LATENCY_WINDOW_SIZE = 1000


def percentile_nearest_rank(sorted_values: list[float], percentile: float) -> float:
    """Return the nearest-rank percentile from an ascending-sorted list.

    Nearest-rank convention: the smallest sample such that at least
    ``percentile`` percent of all samples are ≤ it, i.e.
    ``sorted_values[ceil(percentile/100 · N) − 1]``. The result is always an
    actually-observed sample, never an interpolated midpoint: p50 of 1..100
    is 50.0 (not 50.5), p99 of 1..100 is 99.0 (p100 is the max). Returns 0.0
    for an empty window — a fresh filter has no latency story to tell yet.
    A module-level pure function (like ``bloom.optimal_m``) so the
    convention is pinned by its own tests.
    """
    if not sorted_values:
        return 0.0
    rank = math.ceil((percentile / 100.0) * len(sorted_values))
    rank = min(max(rank, 1), len(sorted_values))
    return sorted_values[rank - 1]


class FilterMetrics:
    """Operation counters and latency stats for ONE named filter.

    Thread-safe: every mutation and the snapshot read go through the single
    internal ``threading.Lock`` (see module docstring for why that is both
    necessary — event-loop thread plus threadpool workers — and
    sufficient). Averages are lifetime (running sums); percentiles cover a
    bounded window of the most recent ``window_size`` samples per kind.
    """

    def __init__(self, window_size: int = LATENCY_WINDOW_SIZE) -> None:
        self._lock = threading.Lock()
        # Lifetime operation counters.
        self._adds_total = 0
        self._queries_total = 0
        self._positives = 0
        self._negatives = 0
        # Bloom said "probably exists" but authoritative storage said no.
        # Incremented by the two-tier pipeline (C10) when it disproves a
        # positive — in real flows this can never exceed ``positives``,
        # since every observed FP starts life as a recorded positive.
        self._observed_false_positives = 0
        # Lifetime latency accumulators; avg = sum / matching counter.
        self._add_sum_ms = 0.0
        self._query_sum_ms = 0.0
        # Bounded recent-latency windows (oldest sample evicted on append).
        self._add_window: deque[float] = deque(maxlen=window_size)
        self._query_window: deque[float] = deque(maxlen=window_size)

    # ------------------------------------------------------------------ #
    # recording                                                          #
    # ------------------------------------------------------------------ #

    def record_add(self, duration_ms: float) -> None:
        """Record one add operation and how long it took."""
        with self._lock:
            self._adds_total += 1
            self._add_sum_ms += duration_ms
            self._add_window.append(duration_ms)

    def record_query(self, duration_ms: float, positive: bool) -> None:
        """Record one query, its duration, and which way the filter answered.

        ``positive`` means the filter said "probably exists"; ``False`` is
        the provable "definitely not exist" branch.
        """
        with self._lock:
            self._queries_total += 1
            if positive:
                self._positives += 1
            else:
                self._negatives += 1
            self._query_sum_ms += duration_ms
            self._query_window.append(duration_ms)

    def record_false_positive(self) -> None:
        """Count one bloom positive that storage later disproved (C10)."""
        with self._lock:
            self._observed_false_positives += 1

    # ------------------------------------------------------------------ #
    # reading                                                            #
    # ------------------------------------------------------------------ #

    def snapshot(self) -> dict:
        """Return every metric as one plain JSON-ready dict.

        All counters and both windows are copied under a **single** lock
        acquisition so the numbers are mutually consistent; the sorting and
        percentile math then run outside the lock, keeping the hold time at
        a strict copy's worth. Millisecond values are rounded to 4 decimals
        (sub-100ns precision is noise here); counters stay exact ints.
        ``observed_fp_rate`` is ``observed_false_positives / positives``
        with the denominator clamped to 1 so a fresh filter reads 0.0
        instead of dividing by zero.
        """
        with self._lock:
            adds_total = self._adds_total
            queries_total = self._queries_total
            positives = self._positives
            negatives = self._negatives
            observed_fps = self._observed_false_positives
            add_sum_ms = self._add_sum_ms
            query_sum_ms = self._query_sum_ms
            add_samples = list(self._add_window)
            query_samples = list(self._query_window)

        add_samples.sort()
        query_samples.sort()
        return {
            "adds_total": adds_total,
            "queries_total": queries_total,
            "positives": positives,
            "negatives": negatives,
            "observed_false_positives": observed_fps,
            "observed_fp_rate": observed_fps / max(1, positives),
            "avg_add_ms": round(add_sum_ms / adds_total, 4) if adds_total else 0.0,
            "p50_add_ms": round(percentile_nearest_rank(add_samples, 50.0), 4),
            "p99_add_ms": round(percentile_nearest_rank(add_samples, 99.0), 4),
            "avg_query_ms": (
                round(query_sum_ms / queries_total, 4) if queries_total else 0.0
            ),
            "p50_query_ms": round(percentile_nearest_rank(query_samples, 50.0), 4),
            "p99_query_ms": round(percentile_nearest_rank(query_samples, 99.0), 4),
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"FilterMetrics(adds={self._adds_total}, "
            f"queries={self._queries_total}, "
            f"observed_fps={self._observed_false_positives})"
        )


class MetricsRegistry:
    """Thread-safe name → :class:`FilterMetrics` map, auto-creating on get.

    The manager (C7) and API (C8) never construct :class:`FilterMetrics`
    directly; they call ``registry.get("error_logs")`` and the first caller
    wins the creation race under the registry lock. Every later caller gets
    the *same* instance, so no recorded operation can land on an orphaned
    ledger.
    """

    def __init__(self, window_size: int = LATENCY_WINDOW_SIZE) -> None:
        self._lock = threading.Lock()
        self._window_size = window_size
        self._by_name: dict[str, FilterMetrics] = {}

    def get(self, name: str) -> FilterMetrics:
        """Return the metrics for ``name``, creating them on first access."""
        with self._lock:
            metrics = self._by_name.get(name)
            if metrics is None:
                metrics = FilterMetrics(window_size=self._window_size)
                self._by_name[name] = metrics
            return metrics

    def snapshot(self) -> dict[str, dict]:
        """Return ``{filter_name: metrics snapshot}`` for every known filter.

        The name → metrics mapping is copied under the registry lock, then
        each :class:`FilterMetrics` produces its own snapshot under its own
        lock *afterwards* — at most one lock is ever held at a time, so
        there is no lock-ordering story to get wrong.
        """
        with self._lock:
            items = list(self._by_name.items())
        return {name: metrics.snapshot() for name, metrics in items}

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"MetricsRegistry(filters={sorted(self._by_name)})"
