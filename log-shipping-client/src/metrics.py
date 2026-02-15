"""Thread-safe metrics collection and periodic reporting."""

import logging
import sys
import threading

logger = logging.getLogger(__name__)


class Metrics:
    """Thread-safe counters for tracking shipper performance."""

    def __init__(self):
        self._lock = threading.Lock()
        self._sent = 0
        self._failed = 0
        self._latencies: list[float] = []
        self._buffer_samples: list[int] = []

    def record_sent(self, latency_ms: float):
        """Record a successful send with its latency in milliseconds."""
        with self._lock:
            self._sent += 1
            self._latencies.append(latency_ms)

    def record_failed(self):
        """Record a failed send."""
        with self._lock:
            self._failed += 1

    def record_buffer_usage(self, size: int):
        """Record a buffer queue size sample."""
        with self._lock:
            self._buffer_samples.append(size)

    def snapshot_and_reset(self) -> dict:
        """Atomically read all counters and reset them to zero."""
        with self._lock:
            latencies = self._latencies
            buffer_samples = self._buffer_samples

            snapshot = {
                "sent": self._sent,
                "failed": self._failed,
                "avg_latency_ms": (
                    sum(latencies) / len(latencies) if latencies else 0.0
                ),
                "max_latency_ms": max(latencies) if latencies else 0.0,
                "avg_buffer_usage": (
                    sum(buffer_samples) / len(buffer_samples)
                    if buffer_samples
                    else 0.0
                ),
            }

            self._sent = 0
            self._failed = 0
            self._latencies = []
            self._buffer_samples = []

            return snapshot


class MetricsReporter:
    """Background thread that periodically logs metrics summaries."""

    def __init__(
        self,
        metrics: Metrics,
        interval: float,
        shutdown_event: threading.Event,
    ):
        self._metrics = metrics
        self._interval = interval
        self._shutdown = shutdown_event
        self._thread: threading.Thread | None = None

    def start(self):
        """Start the reporter thread."""
        self._thread = threading.Thread(target=self._report_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Signal the reporter to stop."""
        if self._thread:
            self._thread.join(timeout=5)

    def _report_loop(self):
        """Periodically snapshot and log metrics."""
        while not self._shutdown.is_set():
            self._shutdown.wait(self._interval)
            if self._shutdown.is_set():
                break

            snapshot = self._metrics.snapshot_and_reset()
            print(
                f"[metrics] sent={snapshot['sent']} "
                f"failed={snapshot['failed']} "
                f"avg_latency={snapshot['avg_latency_ms']:.1f}ms "
                f"max_latency={snapshot['max_latency_ms']:.1f}ms "
                f"avg_buffer={snapshot['avg_buffer_usage']:.0f}",
                file=sys.stderr,
                flush=True,
            )
