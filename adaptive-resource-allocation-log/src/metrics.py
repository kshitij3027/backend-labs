"""Metric collection and rolling history for the Adaptive Resource Allocation System.

This module exposes two stdlib-light building blocks used by the monitoring loop,
the forecaster, and the dashboard:

* :class:`MetricCollector` — samples the host (via ``psutil``) plus the workload
  model and worker pool into a single canonical snapshot dict.
* :class:`RollingHistory` — an in-memory, time-ordered ring of snapshots with
  windowing and per-field series extraction (for Chart.js / forecasting).

``psutil`` is always sampled NON-BLOCKING (``interval=None``) so it is safe to call
from inside an eventlet/SocketIO green thread without yielding the whole event loop.
The first non-blocking call after process start returns a meaningless ``0.0`` (it has
no prior reference point), so :class:`MetricCollector` "primes" the CPU counters once
on construction; every subsequent ``sample()`` then reports a real delta.
"""

import time
from collections import deque
from typing import Any, Deque, Optional

import psutil


# Canonical snapshot keys, declared once so callers/tests can introspect the schema.
SNAPSHOT_KEYS = (
    "timestamp",
    "cpu_percent",
    "cpu_per_core",
    "memory_percent",
    "load_avg",
    "arrival_rate",
    "workers",
    "capacity_per_worker",
    "effective_utilization",
    "queue_depth",
    "throughput",
    "latency_ms",
)


class MetricCollector:
    """Samples host + workload + worker-pool state into a canonical snapshot.

    The collector is a *reader*: it never mutates the ``load_model`` or
    ``worker_pool`` it is given. Those collaborators are duck-typed (this module
    deliberately does not import them) and only need to provide:

    * ``load_model.arrival_rate(now: float) -> float``
    * ``worker_pool.current() -> int``
    * ``worker_pool.stats() -> dict`` with keys ``queue_depth`` (int),
      ``throughput`` (float), ``latency_ms`` (float), ``capacity`` (float).

    ``config`` only needs a ``capacity_per_worker`` attribute (e.g. a
    :class:`src.config.Settings`).
    """

    def __init__(self, config: Any, load_model: Any, worker_pool: Any) -> None:
        self._config = config
        self._load_model = load_model
        self._worker_pool = worker_pool

        # Prime psutil's CPU counters. The first non-blocking call has no previous
        # measurement to diff against and returns 0.0; we discard it here so the
        # first real sample() yields a meaningful utilization figure.
        try:
            psutil.cpu_percent(interval=None)
            psutil.cpu_percent(percpu=True, interval=None)
        except Exception:  # pragma: no cover - psutil priming should not be fatal
            pass

    def _capacity_per_worker(self) -> float:
        """Read ``capacity_per_worker`` from config defensively (default 0.0)."""
        try:
            return float(getattr(self._config, "capacity_per_worker", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _load_avg() -> list[float]:
        """Return the 1/5/15-minute load averages, or zeros where unavailable.

        ``psutil.getloadavg`` raises on platforms without load-average support
        (notably some Windows configurations), so guard it.
        """
        try:
            return [float(x) for x in psutil.getloadavg()]
        except (AttributeError, OSError, NotImplementedError):
            return [0.0, 0.0, 0.0]

    def sample(self, now: Optional[float] = None) -> dict:
        """Capture and return one canonical metric snapshot.

        Args:
            now: Optional wall-clock timestamp (seconds). Defaults to
                ``time.time()``. Also forwarded to ``load_model.arrival_rate`` so
                the arrival rate and the snapshot share a single clock reading.

        Returns:
            A dict containing every key in :data:`SNAPSHOT_KEYS`.
        """
        if now is None:
            now = time.time()

        # --- Host metrics (all NON-BLOCKING) ---
        cpu_percent = float(psutil.cpu_percent(interval=None))
        cpu_per_core = [float(c) for c in psutil.cpu_percent(percpu=True, interval=None)]
        memory_percent = float(psutil.virtual_memory().percent)
        load_avg = self._load_avg()

        # --- Workload + worker-pool metrics ---
        arrival_rate = float(self._load_model.arrival_rate(now))
        workers = int(self._worker_pool.current())
        capacity_per_worker = self._capacity_per_worker()

        # effective_utilization is intentionally NOT capped at 100: values above
        # 100% signal that demand exceeds provisioned capacity (overload).
        denom = max(1.0, workers * capacity_per_worker)
        effective_utilization = round(arrival_rate / denom * 100.0, 2)

        # stats() may be partial or absent; default every field defensively.
        stats = self._worker_pool.stats() or {}
        queue_depth = int(stats.get("queue_depth", 0) or 0)
        throughput = float(stats.get("throughput", 0.0) or 0.0)
        latency_ms = float(stats.get("latency_ms", 0.0) or 0.0)

        return {
            "timestamp": float(now),
            "cpu_percent": cpu_percent,
            "cpu_per_core": cpu_per_core,
            "memory_percent": memory_percent,
            "load_avg": load_avg,
            "arrival_rate": arrival_rate,
            "workers": workers,
            "capacity_per_worker": capacity_per_worker,
            "effective_utilization": effective_utilization,
            "queue_depth": queue_depth,
            "throughput": throughput,
            "latency_ms": latency_ms,
        }


class RollingHistory:
    """An in-memory, time-ordered store of metric snapshots.

    Snapshots are appended in arrival order and kept in a :class:`collections.deque`.
    All time-based reasoning is *relative to the newest snapshot's timestamp* rather
    than to wall-clock ``time.time()`` — this keeps windowing deterministic in tests
    and robust if samples arrive in batches or the clock is supplied externally.
    """

    def __init__(self, window_minutes: int = 15, retention_hours: int = 24) -> None:
        self.window_minutes = int(window_minutes)
        self.retention_hours = int(retention_hours)
        self._items: Deque[dict] = deque()

    def add(self, snapshot: dict) -> None:
        """Append ``snapshot`` and evict anything older than the retention horizon.

        Eviction compares each entry's ``timestamp`` against the *newest* timestamp,
        so retention is measured from the most recent sample (not from now).
        """
        self._items.append(snapshot)

        newest_ts = snapshot.get("timestamp")
        if newest_ts is None:
            return

        cutoff = float(newest_ts) - self.retention_hours * 3600.0
        # deque is ordered oldest -> newest, so drop from the left while too old.
        while self._items and float(self._items[0].get("timestamp", 0.0)) < cutoff:
            self._items.popleft()

    def window(self, minutes: Optional[int] = None) -> list[dict]:
        """Return snapshots within the last ``minutes`` (default ``window_minutes``).

        The window is anchored on the newest snapshot's timestamp. Returns a list in
        oldest -> newest order; empty if there is no history.
        """
        if not self._items:
            return []

        if minutes is None:
            minutes = self.window_minutes

        newest_ts = float(self._items[-1].get("timestamp", 0.0))
        cutoff = newest_ts - minutes * 60.0
        return [s for s in self._items if float(s.get("timestamp", 0.0)) >= cutoff]

    def series(self, field: str, points: int = 60) -> list[float]:
        """Return the last ``points`` values of ``field`` as floats (oldest -> newest).

        Snapshots missing ``field`` (or with a non-numeric value) are skipped. This
        feeds the forecaster and the dashboard's Chart.js line series.
        """
        if points <= 0:
            return []

        values: list[float] = []
        # Walk newest -> oldest, collecting up to ``points`` usable values, then
        # reverse so the result is oldest -> newest.
        for snapshot in reversed(self._items):
            if field not in snapshot:
                continue
            try:
                values.append(float(snapshot[field]))
            except (TypeError, ValueError):
                continue
            if len(values) >= points:
                break

        values.reverse()
        return values

    def latest(self) -> Optional[dict]:
        """Return the newest snapshot, or ``None`` if the history is empty."""
        if not self._items:
            return None
        return self._items[-1]

    def __len__(self) -> int:
        return len(self._items)
