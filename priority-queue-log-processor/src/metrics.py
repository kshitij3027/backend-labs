"""Metrics tracking with internal counters and Prometheus instrumentation."""

import threading
from collections import deque

from prometheus_client import Counter, Gauge, Histogram

from src.models import LogMessage, Priority

# ---------------------------------------------------------------------------
# Prometheus instruments (module-level singletons to avoid duplicate
# registration errors when multiple MetricsTracker instances are created).
# ---------------------------------------------------------------------------

LOGS_ENQUEUED = Counter(
    "logs_enqueued_total",
    "Total logs enqueued",
    ["priority"],
)
LOGS_PROCESSED = Counter(
    "logs_processed_total",
    "Total logs processed",
    ["priority"],
)
LOGS_DROPPED = Counter(
    "logs_dropped_total",
    "Total logs dropped",
    ["priority"],
)
PROCESSING_DURATION = Histogram(
    "processing_duration_seconds",
    "Processing duration",
    ["priority"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
QUEUE_DEPTH = Gauge(
    "queue_depth",
    "Current queue depth",
    ["priority"],
)
ACTIVE_WORKERS = Gauge(
    "active_workers",
    "Active worker threads",
)


class MetricsTracker:
    """Thread-safe metrics collection backed by internal counters.

    Internal counters are the source of truth for ``get_stats()``.
    Prometheus instruments are updated in parallel for scraping.
    """

    _MAX_TIMES = 1000  # per-priority processing time buffer size

    def __init__(self) -> None:
        self._lock = threading.Lock()

        self._enqueued: dict[Priority, int] = {p: 0 for p in Priority}
        self._processed: dict[Priority, int] = {p: 0 for p in Priority}
        self._dropped: dict[Priority, int] = {p: 0 for p in Priority}
        self._processing_times: dict[Priority, list[float]] = {p: [] for p in Priority}

        self._recent_messages: deque[dict] = deque(maxlen=50)

    # ------------------------------------------------------------------
    # Recording helpers
    # ------------------------------------------------------------------

    def record_enqueued(self, priority: Priority) -> None:
        """Record that a message of *priority* was enqueued."""
        with self._lock:
            self._enqueued[priority] += 1
        LOGS_ENQUEUED.labels(priority=priority.name).inc()

    def record_processed(
        self,
        priority: Priority,
        duration_seconds: float,
        message: LogMessage | None = None,
    ) -> None:
        """Record that a message was processed in *duration_seconds*."""
        with self._lock:
            self._processed[priority] += 1
            times = self._processing_times[priority]
            times.append(duration_seconds)
            if len(times) > self._MAX_TIMES:
                self._processing_times[priority] = times[-self._MAX_TIMES :]

            if message is not None:
                self._recent_messages.append(
                    {
                        "id": message.id,
                        "priority": message.priority.name,
                        "message": message.message,
                        "processing_time_ms": round(duration_seconds * 1000, 2),
                        "timestamp": message.timestamp,
                    }
                )

        LOGS_PROCESSED.labels(priority=priority.name).inc()
        PROCESSING_DURATION.labels(priority=priority.name).observe(duration_seconds)

    def record_dropped(self, priority: Priority) -> None:
        """Record that a message of *priority* was dropped."""
        with self._lock:
            self._dropped[priority] += 1
        LOGS_DROPPED.labels(priority=priority.name).inc()

    # ------------------------------------------------------------------
    # Gauge updates
    # ------------------------------------------------------------------

    def update_queue_depth(self, priority_counts: dict[Priority, int]) -> None:
        """Set Prometheus queue-depth gauges from a per-priority count dict."""
        for priority, count in priority_counts.items():
            QUEUE_DEPTH.labels(priority=priority.name).set(count)

    def update_active_workers(self, count: int) -> None:
        """Set the active-workers Prometheus gauge."""
        ACTIVE_WORKERS.set(count)

    # ------------------------------------------------------------------
    # Stats / queries
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return a point-in-time snapshot of all internal counters."""
        with self._lock:
            enqueued = {p.name: self._enqueued[p] for p in Priority}
            processed = {p.name: self._processed[p] for p in Priority}
            dropped = {p.name: self._dropped[p] for p in Priority}

            processing_times: dict[str, dict[str, float]] = {}
            for p in Priority:
                times = list(self._processing_times[p])
                if times:
                    sorted_times = sorted(times)
                    n = len(sorted_times)
                    avg = sum(sorted_times) / n
                    p95 = sorted_times[int(n * 0.95)] if n > 1 else sorted_times[0]
                    p99 = sorted_times[int(n * 0.99)] if n > 1 else sorted_times[0]
                else:
                    avg = 0.0
                    p95 = 0.0
                    p99 = 0.0

                processing_times[p.name] = {
                    "avg": avg,
                    "p95": p95,
                    "p99": p99,
                }

        return {
            "enqueued": enqueued,
            "processed": processed,
            "dropped": dropped,
            "totals": {
                "enqueued": sum(enqueued.values()),
                "processed": sum(processed.values()),
                "dropped": sum(dropped.values()),
            },
            "processing_times": processing_times,
        }

    def get_recent_messages(self) -> list[dict]:
        """Return the last 50 processed messages (most recent last)."""
        with self._lock:
            return list(self._recent_messages)
