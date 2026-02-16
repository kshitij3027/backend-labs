"""Thread-safe metrics counters for the UDP log server."""

import threading
import time
from collections import defaultdict


class Metrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._total_received = 0
        self._level_counts: dict[str, int] = defaultdict(int)
        self._start_time = time.monotonic()

    def increment(self, level: str):
        """Bump the total and per-level counters."""
        with self._lock:
            self._total_received += 1
            self._level_counts[level.upper()] += 1

    def snapshot(self) -> dict:
        """Return a point-in-time snapshot of all metrics."""
        with self._lock:
            elapsed = time.monotonic() - self._start_time
            total = self._total_received
            distribution = dict(self._level_counts)

        return {
            "total_received": total,
            "level_distribution": distribution,
            "elapsed_seconds": round(elapsed, 2),
            "logs_per_second": round(total / elapsed, 2) if elapsed > 0 else 0.0,
        }
