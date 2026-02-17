"""Metrics collector — thread-safe counters and histograms for batch shipping."""

import threading
import time
import logging

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects and reports metrics about batch log shipping operations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._batches_sent: int = 0
        self._total_entries: int = 0
        self._total_bytes: int = 0
        self._batch_sizes: list[int] = []
        self._send_times: list[float] = []
        self._flush_triggers: dict = {"size": 0, "timer": 0}
        self._start_time = time.monotonic()

    def record_batch(
        self,
        batch_size: int,
        bytes_sent: int,
        send_time_ms: float,
        trigger: str = "size",
    ) -> None:
        """Record metrics for a single batch send operation.

        Args:
            batch_size: Number of log entries in the batch.
            bytes_sent: Serialized payload size in bytes.
            send_time_ms: Time taken to send the batch, in milliseconds.
            trigger: What caused the flush — "size" or "timer".
        """
        with self._lock:
            self._batches_sent += 1
            self._total_entries += batch_size
            self._total_bytes += bytes_sent
            self._batch_sizes.append(batch_size)
            self._send_times.append(send_time_ms)
            self._flush_triggers[trigger] = self._flush_triggers.get(trigger, 0) + 1

    def snapshot(self) -> dict:
        """Return a point-in-time snapshot of all collected metrics.

        Returns:
            Dictionary containing counters, averages, percentiles,
            flush trigger counts, and uptime.
        """
        with self._lock:
            batch_sizes = list(self._batch_sizes)
            send_times = list(self._send_times)

            avg_batch = (
                sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0.0
            )
            avg_send = (
                sum(send_times) / len(send_times) if send_times else 0.0
            )

            return {
                "batches_sent": self._batches_sent,
                "total_entries": self._total_entries,
                "total_bytes": self._total_bytes,
                "avg_batch_size": avg_batch,
                "p50_batch_size": self._percentile(batch_sizes, 50),
                "p95_batch_size": self._percentile(batch_sizes, 95),
                "avg_send_time_ms": avg_send,
                "p95_send_time_ms": self._percentile(send_times, 95),
                "flush_triggers": dict(self._flush_triggers),
                "uptime_seconds": time.monotonic() - self._start_time,
            }

    @staticmethod
    def _percentile(data: list, pct: float) -> float:
        """Compute an interpolated percentile from a list of numeric values.

        Args:
            data: List of numeric values (will be sorted internally).
            pct: Desired percentile (0-100).

        Returns:
            Interpolated value at the given percentile, or 0.0 if data is empty.
        """
        if not data:
            return 0.0

        sorted_data = sorted(data)
        n = len(sorted_data)

        if n == 1:
            return float(sorted_data[0])

        idx = (pct / 100) * (n - 1)
        lower = int(idx)
        upper = lower + 1
        fraction = idx - lower

        if upper >= n:
            return float(sorted_data[-1])

        return float(sorted_data[lower] + fraction * (sorted_data[upper] - sorted_data[lower]))
