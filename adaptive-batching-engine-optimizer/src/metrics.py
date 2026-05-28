from __future__ import annotations

from collections import deque
from dataclasses import dataclass

import psutil

from src.models import MetricSnapshot
from src.settings import get_settings


@dataclass(slots=True)
class ResourceReading:
    """Lightweight, immutable result of a single :class:`ResourceMonitor` sample."""

    cpu_percent: float
    memory_percent: float
    memory_available_mb: float


class ResourceMonitor:
    """Reads real host CPU / memory via ``psutil`` without ever blocking.

    ``psutil.cpu_percent(interval=None)`` is non-blocking but reports the delta
    *since the previous call*; the very first call therefore returns ``0.0``.
    The constructor primes that delta by issuing one throwaway call so that the
    first :meth:`sample` returns a meaningful figure. No method here passes a
    positive ``interval`` and none ever sleeps, keeping sampling hot-path safe.
    """

    def __init__(self) -> None:
        # Prime the CPU delta counter; this first read is discarded (returns 0.0).
        psutil.cpu_percent(interval=None)

    def sample(self) -> ResourceReading:
        """Return a non-blocking snapshot of CPU %, memory %, and available MB."""
        vm = psutil.virtual_memory()
        return ResourceReading(
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=vm.percent,
            memory_available_mb=vm.available / (1024**2),
        )


class MetricsCollector:
    """Rolling time-series buffer of :class:`MetricSnapshot` plus query helpers.

    Backed by a bounded ``collections.deque`` (oldest snapshots drop off once
    ``maxlen`` is exceeded). Every write is O(1) so the control loop can record
    well above the spec's 50 samples/sec target. Also tracks the latest known
    queue depth so callers can stamp snapshots consistently.
    """

    def __init__(self, maxlen: int | None = None) -> None:
        size = maxlen or get_settings().metrics_history_size
        self._snapshots: deque[MetricSnapshot] = deque(maxlen=size)
        self._queue_depth: int = 0

    def record(self, snapshot: MetricSnapshot) -> None:
        """Append a pre-built snapshot to the buffer (O(1))."""
        self._snapshots.append(snapshot)

    def record_metrics(
        self,
        *,
        timestamp: float,
        batch_size: int,
        throughput: float,
        latency_ms: float,
        cpu_percent: float,
        memory_percent: float,
        memory_available_mb: float,
        queue_depth: int = 0,
    ) -> MetricSnapshot:
        """Build a :class:`MetricSnapshot` from raw fields, store it, and return it."""
        snapshot = MetricSnapshot(
            timestamp=timestamp,
            batch_size=batch_size,
            throughput=throughput,
            latency_ms=latency_ms,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_available_mb=memory_available_mb,
            queue_depth=queue_depth,
        )
        self._snapshots.append(snapshot)
        return snapshot

    def latest(self) -> MetricSnapshot | None:
        """Return the most recent snapshot, or ``None`` if the buffer is empty."""
        return self._snapshots[-1] if self._snapshots else None

    def snapshot(self, last_n: int | None = None) -> list[MetricSnapshot]:
        """Return the last ``last_n`` snapshots (or all when ``None``), oldest→newest."""
        items = list(self._snapshots)
        if last_n is None or last_n >= len(items):
            return items
        return items[-last_n:] if last_n > 0 else []

    def recent_throughput(self, last_n: int = 5) -> float:
        """Return the mean throughput over the last ``last_n`` snapshots (0.0 if empty)."""
        window = self.snapshot(last_n)
        if not window:
            return 0.0
        return sum(s.throughput for s in window) / len(window)

    def to_series(self, last_n: int | None = None) -> dict[str, list]:
        """Return parallel lists for charting, keyed by metric, over the last_n points."""
        window = self.snapshot(last_n)
        return {
            "timestamp": [s.timestamp for s in window],
            "batch_size": [s.batch_size for s in window],
            "throughput": [s.throughput for s in window],
            "latency_ms": [s.latency_ms for s in window],
            "cpu_percent": [s.cpu_percent for s in window],
            "memory_percent": [s.memory_percent for s in window],
        }

    def set_queue_depth(self, depth: int) -> None:
        """Update the current queue depth tracked alongside the buffer."""
        self._queue_depth = int(depth)

    @property
    def queue_depth(self) -> int:
        """Most recently recorded queue depth."""
        return self._queue_depth

    def clear(self) -> None:
        """Empty the buffer (used by the optimizer reset)."""
        self._snapshots.clear()

    def __len__(self) -> int:
        return len(self._snapshots)
