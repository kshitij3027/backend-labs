"""Per-second metric time series in fixed numpy ring buffers.

:class:`MetricAggregator` folds the parsed :class:`~src.models.LogEvent` stream
into per-second series over a 120-slot ring (slot = ``second % 120``). This is
the pipeline's hot path: :meth:`add_event` does only scalar writes into
pre-allocated float64 buffers — no numpy allocation, no pydantic work — so
1000+ events/sec bursts stay cheap. Reads (:meth:`series`/:meth:`aligned`) may
allocate freely; the detectors only call them a few times per 2-second cycle.

Storage model:

- **count series** accumulate directly (an empty second reads 0.0);
- **averaged series** keep SUM + COUNT buffer pairs and divide on read (an
  empty second reads NaN — never a fake 0.0 latency);
- **ratio series** (``web.error_rate``, ``db.pool_utilization``) are derived on
  read from the buffers above (NaN when the denominator is zero/unknown);
- **error-presence binaries** per source: 1.0 for any second with >=1
  ERROR-level event (Jaccard/co-occurrence input for the C6 metric detector).

The ring head (the newest, in-progress second) advances via :meth:`roll` (the
trusted pipeline clock) or by newer event timestamps; every slot swept past is
reset so it can never leak a value from 120 seconds ago. Events older than the
window or implausibly far in the future (> 5s ahead of the head) are dropped —
one corrupt timestamp must not alias into, or wipe, the ring.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from src.models import (
    CART_ABANDONED,
    CHECKOUT_FAILED,
    INVENTORY_TIMEOUT,
    LogEvent,
    SourceType,
)

#: Ring size: one slot per second (reads see at most 119 completed seconds).
WINDOW_SLOTS = 120
#: Events stamped more than this far ahead of the ring head are dropped.
FUTURE_TOLERANCE_SECONDS = 5

#: Count series — accumulated directly; an empty second reads 0.0.
_COUNT_SERIES: tuple[str, ...] = (
    "web.request_count",
    "web.error_5xx_count",
    "db.query_count",
    "db.error_count",
    "api.request_count",
    "api.error_count",
    "payment.txn_count",
    "payment.error_count",
    "inventory.op_count",
    "inventory.timeout_count",
    "checkout.failure_count",
    "user.abandonment_count",
)
#: Averaged series — SUM + COUNT buffer pairs; an empty second reads NaN.
_AVG_SERIES: tuple[str, ...] = (
    "web.latency_ms_avg",
    "db.pool_in_use_avg",
    "api.latency_ms_avg",
    "payment.latency_ms_avg",
    "inventory.latency_ms_avg",
)
#: Ratio series — derived on read from other buffers; NaN when undefined.
_RATIO_SERIES: tuple[str, ...] = ("web.error_rate", "db.pool_utilization")

#: Public registry: every name resolvable by :meth:`MetricAggregator.series`.
SERIES: tuple[str, ...] = _COUNT_SERIES + _AVG_SERIES + _RATIO_SERIES


class MetricAggregator:
    """Per-second metric rings over the event stream (see module docstring).

    Single-threaded by design: one pipeline task owns all writes, and reads
    happen between ticks on the same loop — no locking anywhere.
    """

    def __init__(self) -> None:
        #: First second ever observed (event or roll); None until data arrives.
        self._base_sec: int | None = None
        #: Ring head — the newest (in-progress) second. Reads exclude it.
        self._last_sec = 0

        self._counts = {name: np.zeros(WINDOW_SLOTS) for name in _COUNT_SERIES}
        self._sums = {name: np.zeros(WINDOW_SLOTS) for name in _AVG_SERIES}
        self._ns = {name: np.zeros(WINDOW_SLOTS) for name in _AVG_SERIES}
        self._presence = {source.value: np.zeros(WINDOW_SLOTS) for source in SourceType}
        #: Last pool_size reported by a db line (the pool is fixed-size, so one
        #: remembered scalar suffices; None = never seen -> utilization is NaN).
        self._pool_size: float | None = None
        #: Every ring buffer, for the roll sweep.
        self._all_buffers: list[np.ndarray] = [
            *self._counts.values(),
            *self._sums.values(),
            *self._ns.values(),
            *self._presence.values(),
        ]

        # Hot-path aliases: attribute loads beat per-event dict lookups in
        # add_event (these reference the SAME arrays as the dicts above).
        counts, sums, ns = self._counts, self._sums, self._ns
        self._c_web_req = counts["web.request_count"]
        self._c_web_5xx = counts["web.error_5xx_count"]
        self._c_db_query = counts["db.query_count"]
        self._c_db_err = counts["db.error_count"]
        self._c_api_req = counts["api.request_count"]
        self._c_api_err = counts["api.error_count"]
        self._c_pay_txn = counts["payment.txn_count"]
        self._c_pay_err = counts["payment.error_count"]
        self._c_inv_op = counts["inventory.op_count"]
        self._c_inv_timeout = counts["inventory.timeout_count"]
        self._c_checkout_fail = counts["checkout.failure_count"]
        self._c_abandon = counts["user.abandonment_count"]
        self._s_web_lat, self._n_web_lat = sums["web.latency_ms_avg"], ns["web.latency_ms_avg"]
        self._s_db_pool, self._n_db_pool = sums["db.pool_in_use_avg"], ns["db.pool_in_use_avg"]
        self._s_api_lat, self._n_api_lat = sums["api.latency_ms_avg"], ns["api.latency_ms_avg"]
        self._s_pay_lat, self._n_pay_lat = sums["payment.latency_ms_avg"], ns["payment.latency_ms_avg"]
        self._s_inv_lat, self._n_inv_lat = sums["inventory.latency_ms_avg"], ns["inventory.latency_ms_avg"]

    # --- Writes (hot path) --------------------------------------------------------
    def add_event(self, ev: LogEvent) -> None:
        """Fold one parsed event into its second's bucket (scalar writes only)."""
        sec = int(ev.timestamp)
        if self._base_sec is None:
            self._base_sec = sec
            self._last_sec = sec
        elif sec > self._last_sec:
            # A newer second advances the head — unless the timestamp is
            # implausibly far ahead (a corrupt line must not sweep the ring).
            if sec - self._last_sec > FUTURE_TOLERANCE_SECONDS:
                return
            self._advance_to(sec)
        elif self._last_sec - sec >= WINDOW_SLOTS:
            return  # older than the ring — its slot now belongs to a newer second

        idx = sec % WINDOW_SLOTS
        metrics = ev.metrics
        source = ev.source
        if source is SourceType.WEB:
            self._c_web_req[idx] += 1.0
            status = metrics.get("status")
            if status is not None and status >= 500.0:
                self._c_web_5xx[idx] += 1.0
            latency = metrics.get("latency_ms")
            if latency is not None:
                self._s_web_lat[idx] += latency
                self._n_web_lat[idx] += 1.0
            if ev.error_code == CART_ABANDONED:
                self._c_abandon[idx] += 1.0
        elif source is SourceType.DATABASE:
            self._c_db_query[idx] += 1.0
            pool_in_use = metrics.get("pool_in_use")
            if pool_in_use is not None:
                self._s_db_pool[idx] += pool_in_use
                self._n_db_pool[idx] += 1.0
            pool_size = metrics.get("pool_size")
            if pool_size:  # 0/None are both "unknown"
                self._pool_size = pool_size
            if ev.level == "ERROR":
                self._c_db_err[idx] += 1.0
        elif source is SourceType.API_SERVICE:
            self._c_api_req[idx] += 1.0
            latency = metrics.get("latency_ms")
            if latency is not None:
                self._s_api_lat[idx] += latency
                self._n_api_lat[idx] += 1.0
            if ev.level == "ERROR":
                self._c_api_err[idx] += 1.0
            if ev.error_code == CHECKOUT_FAILED:
                self._c_checkout_fail[idx] += 1.0
        elif source is SourceType.PAYMENT:
            self._c_pay_txn[idx] += 1.0
            latency = metrics.get("latency_ms")
            if latency is not None:
                self._s_pay_lat[idx] += latency
                self._n_pay_lat[idx] += 1.0
            if ev.level == "ERROR":
                self._c_pay_err[idx] += 1.0
        else:  # SourceType.INVENTORY
            self._c_inv_op[idx] += 1.0
            latency = metrics.get("latency_ms")
            if latency is not None:
                self._s_inv_lat[idx] += latency
                self._n_inv_lat[idx] += 1.0
            if ev.error_code == INVENTORY_TIMEOUT:
                self._c_inv_timeout[idx] += 1.0

        if ev.level == "ERROR":
            self._presence[source.value][idx] = 1.0

    def roll(self, now: float) -> None:
        """Advance the ring head to ``int(now)``, resetting every slot swept past."""
        sec = int(now)
        if self._base_sec is None:
            self._base_sec = sec
            self._last_sec = sec
        elif sec > self._last_sec:
            self._advance_to(sec)

    def _advance_to(self, sec: int) -> None:
        """Move the head forward to ``sec``, zeroing each newly entered slot."""
        if sec - self._last_sec >= WINDOW_SLOTS:
            # The gap swallows the whole ring: one bulk reset instead of a
            # per-slot sweep (this caps the sweep at 120 slots of work).
            for buf in self._all_buffers:
                buf.fill(0.0)
        else:
            for entered in range(self._last_sec + 1, sec + 1):
                idx = entered % WINDOW_SLOTS
                for buf in self._all_buffers:
                    buf[idx] = 0.0
        self._last_sec = sec

    # --- Reads (detector path — allocation is fine here) ---------------------------
    def series(self, name: str, n: int = 60) -> np.ndarray:
        """Last ``n`` COMPLETED seconds of ``name``, oldest -> newest.

        The in-progress head second is excluded (it would systematically
        under-count). Counts default to 0.0 and averages/ratios to NaN wherever
        a second saw no matching samples (or predates all data).
        """
        return self._gather(name, self._clamp_n(n))

    def aligned(self, names: Sequence[str], n: int = 60) -> dict[str, np.ndarray]:
        """Several series over the SAME n-second window (pairwise-correlation input)."""
        n = self._clamp_n(n)
        return {name: self._gather(name, n) for name in names}

    def error_presence(self, source: str | SourceType, n: int = 60) -> np.ndarray:
        """Binary series: 1.0 for each completed second with >=1 ERROR from ``source``."""
        key = source.value if isinstance(source, SourceType) else str(source)
        buf = self._presence[key]  # KeyError on unknown source = programmer error
        n = self._clamp_n(n)
        if self._base_sec is None:
            return np.zeros(n)
        return buf[self._window_idxs(n)]

    @staticmethod
    def current_second(now: float) -> int:
        """The (in-progress, read-excluded) bucket second for wall-clock ``now``."""
        return int(now)

    # --- Read internals -------------------------------------------------------------
    @staticmethod
    def _clamp_n(n: int) -> int:
        # A 120-slot ring holds at most 119 completed seconds besides the head.
        return max(1, min(int(n), WINDOW_SLOTS - 1))

    def _window_idxs(self, n: int) -> np.ndarray:
        """Ring indices of the last ``n`` completed seconds, oldest -> newest."""
        secs = np.arange(self._last_sec - n, self._last_sec, dtype=np.int64)
        return secs % WINDOW_SLOTS

    def _gather(self, name: str, n: int) -> np.ndarray:
        counts = self._counts.get(name)
        if self._base_sec is None:
            # No data ever: counts read as all-zero, everything else as NaN.
            return np.zeros(n) if counts is not None else np.full(n, np.nan)
        idxs = self._window_idxs(n)
        if counts is not None:
            return counts[idxs]  # fancy indexing copies — callers cannot alias the ring
        if name in self._sums:
            return self._ratio(self._sums[name][idxs], self._ns[name][idxs])
        if name == "web.error_rate":
            return self._ratio(
                self._counts["web.error_5xx_count"][idxs],
                self._counts["web.request_count"][idxs],
            )
        if name == "db.pool_utilization":
            if self._pool_size is None or self._pool_size <= 0.0:
                return np.full(n, np.nan)
            return self._ratio(self._s_db_pool[idxs], self._n_db_pool[idxs]) / self._pool_size
        raise KeyError(f"unknown series {name!r} (see aggregation.SERIES)")

    @staticmethod
    def _ratio(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
        """``numer / denom`` with NaN wherever ``denom`` is 0 (empty second)."""
        out = np.full(numer.shape, np.nan)
        np.divide(numer, denom, out=out, where=denom > 0.0)
        return out
