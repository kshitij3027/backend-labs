"""Prometheus metrics and internal throughput tracking."""

import time
import threading
from collections import defaultdict, deque

from prometheus_client import Counter, Histogram, Gauge


# Module-level Prometheus metrics (registered once per process)
messages_sent_total = Counter(
    "messages_sent_total",
    "Total messages sent",
    ["topic", "status"],
)
send_latency_seconds = Histogram(
    "send_latency_seconds",
    "Message send latency",
    ["topic"],
)
buffer_available_bytes = Gauge(
    "buffer_available_bytes",
    "Available producer buffer bytes",
)


class ProducerMetrics:
    """Thread-safe metrics collector combining Prometheus exports with
    internal throughput and error tracking."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Rolling window: (timestamp, count) tuples — last 60 seconds
        self._throughput_window: deque[tuple[float, int]] = deque(maxlen=60)
        self._total_sent: int = 0
        self._total_failed: int = 0
        self._topic_counts: dict[str, int] = defaultdict(int)
        self._error_counts: dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Recording helpers
    # ------------------------------------------------------------------

    def record_success(self, topic: str, latency: float) -> None:
        """Record a successfully delivered message."""
        messages_sent_total.labels(topic=topic, status="success").inc()
        send_latency_seconds.labels(topic=topic).observe(latency)

        with self._lock:
            self._total_sent += 1
            self._topic_counts[topic] += 1

    def record_failure(self, topic: str, error_type: str) -> None:
        """Record a failed delivery attempt."""
        messages_sent_total.labels(topic=topic, status="error").inc()

        with self._lock:
            self._total_failed += 1
            self._error_counts[error_type] += 1

    def record_throughput(self, count: int) -> None:
        """Append a throughput sample to the rolling window."""
        with self._lock:
            self._throughput_window.append((time.time(), count))

    def update_buffer(self, available_bytes: int) -> None:
        """Update the Prometheus gauge for available buffer space."""
        buffer_available_bytes.set(available_bytes)

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    @property
    def snapshot(self) -> dict:
        """Return a thread-safe dict summarising current metrics."""
        with self._lock:
            total = self._total_sent + self._total_failed
            error_rate = (
                (self._total_failed / total * 100.0) if total > 0 else 0.0
            )

            # Calculate throughput (msgs/sec) over the window
            if len(self._throughput_window) >= 2:
                first_ts = self._throughput_window[0][0]
                last_ts = self._throughput_window[-1][0]
                total_count = sum(c for _, c in self._throughput_window)
                elapsed = last_ts - first_ts
                throughput = total_count / elapsed if elapsed > 0 else 0.0
            elif len(self._throughput_window) == 1:
                throughput = float(self._throughput_window[0][1])
            else:
                throughput = 0.0

            return {
                "total_sent": self._total_sent,
                "total_failed": self._total_failed,
                "topic_counts": dict(self._topic_counts),
                "throughput": throughput,
                "error_counts": dict(self._error_counts),
                "error_rate": error_rate,
            }
