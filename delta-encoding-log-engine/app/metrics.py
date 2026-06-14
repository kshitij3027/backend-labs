"""Runtime operational metrics — throughput, reconstruction latency, errors, uptime.

This is the **live-operation** observability layer, deliberately kept separate from
``app.store``'s byte accounting. :class:`~app.store.CompressionStats` answers "how
many bytes did delta encoding save?" — a property of the *data*. This module answers
"how fast is the engine serving requests, and is it healthy?" — a property of the
*process*: per-operation latency percentiles, entries-per-second throughput, a global
error counter, and wall-clock-independent uptime. The two never overlap; nothing here
re-derives a compression ratio.

What feeds the success-criteria gates
-------------------------------------
*plan.md* states three operational targets that this registry measures directly:

* **<100ms reconstruction latency per entry** — the ``reconstruct`` op's latency
  percentiles (``p50``/``p99``), surfaced as the top-level ``reconstruct_p50_ms`` /
  ``reconstruct_p99_ms`` convenience keys in :meth:`MetricsRegistry.snapshot`.
* **>100 entries/s processing** and **≥1000 entries/s compression** — the
  ``compress`` op's :meth:`MetricsRegistry.throughput_eps`, surfaced as
  ``compress_throughput_eps``.
* **``system.errors == 0`` reliability gate** — the global :attr:`errors` counter,
  bumped by :meth:`incr_error` from any handler that catches a failure.

Why latency windows are bounded
-------------------------------
Each op's samples live in a ``collections.deque(maxlen=max_samples)``. A bounded deque
evicts its oldest sample on every append once full, so the memory cost of latency
tracking is fixed forever — after a million operations the ``reconstruct`` window still
holds exactly ``max_samples`` floats. Percentiles therefore describe the most-recent
window (the rolling view a live dashboard wants), while throughput uses lifetime
running totals (``entries_total`` / ``time_total``) and so covers the whole run in O(1)
extra space. An unbounded sample list is exactly the slow-burn leak a long-lived single
process must avoid.

Why a single lock — and why ``time_block`` times *outside* it
-------------------------------------------------------------
Heavy API handlers (``/api/compress``, ``/api/reconstruct``) run in Starlette's AnyIO
threadpool while trivial handlers (``/api/stats``) run on the event-loop thread, so a
real :class:`threading.Lock` (not an ``asyncio.Lock``) is required to keep the shared
state consistent across threads. One lock suffices: every critical section is a handful
of int/float bumps plus a deque append — tens of nanoseconds uncontended — so sharding
would buy nothing. Crucially, :meth:`MetricsRegistry.time_block` measures the wrapped
operation with ``perf_counter`` *outside* the lock and only takes the lock for the final
:meth:`record`; a slow compress/reconstruct never serializes the registry against the
fast ``/api/stats`` readers.

Why linear-interpolation percentiles
------------------------------------
``percentiles`` uses the linear-interpolation-between-closest-ranks definition (numpy's
default ``method="linear"``): position ``(N-1)·P/100`` into the sorted window, then
interpolate between the two bracketing samples. This makes the ordering guarantee
``p50 <= p90 <= p99 <= max`` fall out of the percentile being a monotonic function of P
on a fixed sorted window, which the latency gates rely on. ``time.perf_counter`` /
``time.monotonic`` are used for timing and uptime so neither is perturbed by a
wall-clock (NTP) adjustment mid-run.
"""
from __future__ import annotations

import collections
import threading
import time
from contextlib import contextmanager

#: Default per-operation latency window length. Large enough for a stable p99 while
#: pinning each op's window at a fixed ~8 KB of floats regardless of uptime.
DEFAULT_MAX_SAMPLES = 1024

#: The zero-valued latency block returned for an op with no samples yet, so callers
#: (snapshot, the dashboard) can read every key unconditionally — never a KeyError.
_EMPTY_LATENCY: dict = {
    "count": 0,
    "p50_ms": 0.0,
    "p90_ms": 0.0,
    "p99_ms": 0.0,
    "mean_ms": 0.0,
    "max_ms": 0.0,
}


def _percentile_linear(sorted_values: list[float], percentile: float) -> float:
    """Linear-interpolation percentile of an ascending-sorted list (numpy default).

    Computes the value at fractional rank ``(N-1)·percentile/100`` and linearly
    interpolates between the two samples that bracket it — identical to numpy's
    ``method="linear"``. Because, for a fixed sorted window, this is a monotonically
    non-decreasing function of ``percentile``, ``p50 <= p90 <= p99 <= max`` holds
    automatically. Returns ``0.0`` for an empty window. Pure module-level function so
    the convention is pinned by its own tests.
    """
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_values[0]
    # Fractional rank into [0, n-1]; clamp percentile defensively to [0, 100].
    p = min(max(percentile, 0.0), 100.0)
    rank = (p / 100.0) * (n - 1)
    lo = int(rank)  # floor of the fractional rank
    if lo >= n - 1:
        return sorted_values[n - 1]
    frac = rank - lo
    return sorted_values[lo] + frac * (sorted_values[lo + 1] - sorted_values[lo])


class _OpState:
    """Mutable per-operation accumulators (latency window + lifetime totals).

    Holds no lock of its own — every access is serialized by the owning
    :class:`MetricsRegistry`'s single lock. Latencies are stored in **seconds**
    (``perf_counter`` units) and converted to milliseconds only at report time.
    """

    __slots__ = ("latencies", "call_count", "entries_total", "time_total")

    def __init__(self, max_samples: int) -> None:
        # Bounded window of recent per-call latencies, in SECONDS (oldest evicted).
        self.latencies: collections.deque[float] = collections.deque(maxlen=max_samples)
        self.call_count: int = 0
        self.entries_total: int = 0
        self.time_total: float = 0.0  # lifetime sum of seconds, for throughput


class MetricsRegistry:
    """Thread-safe registry of per-operation latency, throughput, and global errors.

    Operations (e.g. ``"compress"``, ``"reconstruct"``) are created lazily on first
    :meth:`record`; querying an unseen op returns the empty/zero result rather than
    raising. All shared state lives behind one :class:`threading.Lock`; latency windows
    are bounded by ``max_samples`` so memory is fixed for the life of the process. Use
    :meth:`time_block` to time API handlers (it does the timing outside the lock).
    """

    def __init__(self, *, max_samples: int = DEFAULT_MAX_SAMPLES) -> None:
        """Create an empty registry. ``max_samples`` bounds each op's latency window."""
        self._max_samples = max_samples
        self._start = time.monotonic()
        self._ops: dict[str, _OpState] = {}
        self._errors: int = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # recording                                                          #
    # ------------------------------------------------------------------ #
    def record(self, op: str, *, entries: int, seconds: float) -> None:
        """Record one ``op`` invocation that processed ``entries`` in ``seconds``.

        Appends ``seconds`` to the op's bounded latency window and folds the call into
        its lifetime totals (``call_count`` += 1, ``entries_total`` += ``entries``,
        ``time_total`` += ``seconds``). Used for both ``compress`` (``entries`` = batch
        size) and ``reconstruct`` (``entries`` = 1 for a single-entry reconstruct). The
        op is created on first use, so any name is valid.
        """
        with self._lock:
            state = self._ops.get(op)
            if state is None:
                state = _OpState(self._max_samples)
                self._ops[op] = state
            state.latencies.append(seconds)
            state.call_count += 1
            state.entries_total += entries
            state.time_total += seconds

    def record_latency(self, op: str, seconds: float) -> None:
        """Convenience for a single-entry op: ``record(op, entries=1, seconds=...)``."""
        self.record(op, entries=1, seconds=seconds)

    def incr_error(self, n: int = 1) -> None:
        """Add ``n`` (default 1) to the global error counter (the ``system.errors`` gate)."""
        with self._lock:
            self._errors += n

    @contextmanager
    def time_block(self, op: str, entries: int = 1):
        """Context manager timing a ``with`` block via ``perf_counter`` and recording it.

        On exit, records ``record(op, entries=entries, seconds=elapsed)``. The timing is
        captured **outside** the registry lock — only the final :meth:`record` acquires
        it — so a slow wrapped operation never serializes other threads against the
        registry. ``perf_counter`` is monotonic and high-resolution, immune to
        wall-clock adjustments. The recording runs even if the block raises, so a failed
        operation still contributes its latency (callers pair this with :meth:`incr_error`).

        Usage::

            with metrics.time_block("compress", entries=len(batch)):
                stats = store.compress(batch)
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.record(op, entries=entries, seconds=elapsed)

    # ------------------------------------------------------------------ #
    # reading                                                            #
    # ------------------------------------------------------------------ #
    def percentiles(self, op: str) -> dict:
        """Return latency percentiles (in **ms**) for ``op`` over its recent window.

        Shape: ``{"count", "p50_ms", "p90_ms", "p99_ms", "mean_ms", "max_ms"}``. The
        deque is snapshotted and copied under the lock, then sorted and reduced *outside*
        it (keeping the hold time to a copy's worth). Seconds are converted to ms and
        rounded to 3 decimals. An empty / unknown op yields all-zeros with ``count`` 0.
        For non-trivial windows the linear-interpolation percentile guarantees
        ``p50_ms <= p90_ms <= p99_ms <= max_ms``.
        """
        with self._lock:
            state = self._ops.get(op)
            samples = list(state.latencies) if state is not None else []

        if not samples:
            return dict(_EMPTY_LATENCY)

        samples.sort()
        mean_s = sum(samples) / len(samples)
        return {
            "count": len(samples),
            "p50_ms": round(_percentile_linear(samples, 50.0) * 1000.0, 3),
            "p90_ms": round(_percentile_linear(samples, 90.0) * 1000.0, 3),
            "p99_ms": round(_percentile_linear(samples, 99.0) * 1000.0, 3),
            "mean_ms": round(mean_s * 1000.0, 3),
            "max_ms": round(samples[-1] * 1000.0, 3),
        }

    def throughput_eps(self, op: str) -> float:
        """Lifetime entries-per-second for ``op`` = ``entries_total / time_total``.

        Uses lifetime running totals (not the bounded window), so it reflects the whole
        run. Returns ``0.0`` when ``time_total == 0`` (no recorded time yet, or an
        unknown op). Rounded to 2 decimals. This is what the ≥1000 eps compression and
        >100 eps processing gates read.
        """
        with self._lock:
            state = self._ops.get(op)
            if state is None or state.time_total == 0.0:
                return 0.0
            return round(state.entries_total / state.time_total, 2)

    def uptime_seconds(self) -> float:
        """Seconds since construction (or last :meth:`reset`), via ``time.monotonic``.

        ``monotonic`` never goes backwards and is unaffected by system clock changes, so
        uptime is reliable even across an NTP step. Rounded to 3 decimals.
        """
        return round(time.monotonic() - self._start, 3)

    @property
    def errors(self) -> int:
        """Current value of the global error counter (the ``system.errors`` gate)."""
        with self._lock:
            return self._errors

    def snapshot(self) -> dict:
        """Return one JSON-native dict of all live metrics, shaped for ``/api/stats``.

        Combines uptime, the error counter, a per-op block
        (``calls``/``entries``/``throughput_eps``/``latency_ms``) for every recorded op,
        and top-level convenience keys the success-criteria gates read directly:
        ``reconstruct_p50_ms``, ``reconstruct_p99_ms``, ``compress_throughput_eps``
        (each ``0.0`` when its op has no samples yet). The op names are copied under the
        lock; the per-op reductions then run via :meth:`percentiles` / :meth:`throughput_eps`
        (each taking the lock briefly on its own) so at most one lock is held at a time.
        """
        # Snapshot the set of op names + their cheap lifetime counters under the lock;
        # the heavier sort/percentile work happens afterwards via the public readers.
        with self._lock:
            op_counts = {
                op: (state.call_count, state.entries_total)
                for op, state in self._ops.items()
            }
            errors = self._errors

        operations: dict[str, dict] = {}
        for op, (calls, entries) in op_counts.items():
            operations[op] = {
                "calls": calls,
                "entries": entries,
                "throughput_eps": self.throughput_eps(op),
                "latency_ms": self.percentiles(op),
            }

        # Convenience hooks for the latency / throughput gates. Read straight off the
        # computed reconstruct / compress blocks (zero when the op has no samples).
        reconstruct_latency = operations.get("reconstruct", {}).get(
            "latency_ms", _EMPTY_LATENCY
        )
        compress_block = operations.get("compress", {})
        return {
            "uptime_seconds": self.uptime_seconds(),
            "errors": errors,
            "operations": operations,
            "reconstruct_p50_ms": reconstruct_latency["p50_ms"],
            "reconstruct_p99_ms": reconstruct_latency["p99_ms"],
            "compress_throughput_eps": compress_block.get("throughput_eps", 0.0),
        }

    def reset(self) -> None:
        """Clear every op's state and the error counter, and restart the uptime clock.

        Thread-safe: all mutation happens under the lock. After this the registry reads
        as freshly constructed — no ops, zero errors, ``uptime_seconds`` counting from
        now. Mirrors ``SegmentStore.reset`` so ``/api/reset`` zeroes both layers together.
        """
        with self._lock:
            self._ops.clear()
            self._errors = 0
            self._start = time.monotonic()
