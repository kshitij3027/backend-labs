"""Thread-safe metrics collection for the log producer."""

import threading
import time
from collections import deque


class MetricsCollector:
    """Collects and exposes metrics for the log producer pipeline."""

    def __init__(self):
        self._lock = threading.Lock()
        self._messages_received = 0
        self._messages_published = 0
        self._batches_flushed = 0
        self._publish_errors = 0
        self._fallback_writes = 0
        self._fallback_drained = 0
        self._latencies = deque(maxlen=1000)
        self._start_time = time.monotonic()

    def record_received(self, count=1):
        with self._lock:
            self._messages_received += count

    def record_published(self, count=1):
        with self._lock:
            self._messages_published += count

    def record_batch_flushed(self):
        with self._lock:
            self._batches_flushed += 1

    def record_publish_error(self):
        with self._lock:
            self._publish_errors += 1

    def record_fallback_write(self, count=1):
        with self._lock:
            self._fallback_writes += count

    def record_fallback_drained(self, count=1):
        with self._lock:
            self._fallback_drained += count

    def record_latency(self, ms):
        with self._lock:
            self._latencies.append(ms)

    def get_throughput(self):
        """Messages published per second since start."""
        with self._lock:
            elapsed = time.monotonic() - self._start_time
            if elapsed <= 0:
                return 0.0
            return self._messages_published / elapsed

    def get_latency_p95(self):
        """95th percentile latency in milliseconds."""
        with self._lock:
            if not self._latencies:
                return 0.0
            sorted_latencies = sorted(self._latencies)
            idx = int(len(sorted_latencies) * 0.95)
            idx = min(idx, len(sorted_latencies) - 1)
            return sorted_latencies[idx]

    def snapshot(self):
        """Return a dict of all metrics."""
        with self._lock:
            elapsed = time.monotonic() - self._start_time
            throughput = self._messages_published / elapsed if elapsed > 0 else 0.0
            latency_p95 = 0.0
            if self._latencies:
                sorted_lat = sorted(self._latencies)
                idx = min(int(len(sorted_lat) * 0.95), len(sorted_lat) - 1)
                latency_p95 = sorted_lat[idx]

            return {
                "messages_received": self._messages_received,
                "messages_published": self._messages_published,
                "batches_flushed": self._batches_flushed,
                "publish_errors": self._publish_errors,
                "fallback_writes": self._fallback_writes,
                "fallback_drained": self._fallback_drained,
                "throughput": round(throughput, 2),
                "latency_p95": round(latency_p95, 2),
                "uptime_seconds": round(elapsed, 2),
            }
