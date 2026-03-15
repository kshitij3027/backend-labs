"""Metrics tracker — records throughput and per-topic message counts."""

import threading
import time
from collections import defaultdict, deque

# Throughput is measured over this rolling window (in seconds).
_THROUGHPUT_WINDOW = 10


class MetricsTracker:
    """Tracks throughput, consumer lag, latency, and message counts.  Thread-safe."""

    def __init__(self, bootstrap_servers: str = None):
        self._lock = threading.Lock()
        self._message_timestamps: deque[float] = deque(maxlen=100000)
        self._counts: defaultdict[str, int] = defaultdict(int)
        self._total: int = 0

        # Latency tracking (produce-to-consume)
        self._latency_samples: deque[float] = deque(maxlen=10000)

        # Throughput time-series: last 60 seconds in 1-second buckets
        self._throughput_history: deque[dict] = deque(maxlen=60)

        # Consumer lag per topic-partition
        self._consumer_lag: dict[str, int] = {}

        # Optional AdminClient for lag tracking
        self._admin_client = None
        if bootstrap_servers:
            try:
                from confluent_kafka.admin import AdminClient
                self._admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
            except Exception:
                pass

        # Start background throughput snapshot thread
        self._running = True
        self._snapshot_thread = threading.Thread(
            target=self._throughput_snapshot_loop, daemon=True
        )
        self._snapshot_thread.start()

    def stop(self):
        """Stop the background snapshot thread."""
        self._running = False

    def record_consumed(self, topic: str, latency_ms: float = None):
        """Record that one message was consumed from *topic*.

        Optionally record produce-to-consume latency in milliseconds.
        """
        with self._lock:
            self._message_timestamps.append(time.time())
            self._counts[topic] += 1
            self._total += 1
            if latency_ms is not None:
                self._latency_samples.append(latency_ms)

    def update_consumer_lag(self, consumer):
        """Update consumer lag from a confluent_kafka Consumer instance.

        Calculates lag as high_watermark - committed_offset for each
        assigned partition.
        """
        try:
            assigned = consumer.assignment()
            lag: dict[str, int] = {}
            for tp in assigned:
                # Get committed offset
                committed = consumer.committed([tp], timeout=5)
                if committed and committed[0].offset >= 0:
                    committed_offset = committed[0].offset
                else:
                    committed_offset = 0

                # Get high watermark
                _low, high = consumer.get_watermark_offsets(tp, timeout=5)
                lag[f"{tp.topic}-{tp.partition}"] = max(0, high - committed_offset)

            with self._lock:
                self._consumer_lag = lag
        except Exception:
            pass

    @property
    def throughput(self) -> dict:
        """Return throughput stats over the last 10 seconds."""
        with self._lock:
            now = time.time()
            cutoff = now - _THROUGHPUT_WINDOW
            recent = sum(1 for t in self._message_timestamps if t > cutoff)
            return {
                "messages_per_second": round(recent / _THROUGHPUT_WINDOW, 1),
                "total_messages": self._total,
                "by_topic": dict(self._counts),
            }

    @property
    def consumer_lag(self) -> dict:
        """Return current consumer lag per topic-partition."""
        with self._lock:
            return dict(self._consumer_lag)

    @property
    def latency_stats(self) -> dict:
        """Return p50, p95, p99 latency in ms."""
        with self._lock:
            if not self._latency_samples:
                return {"p50": 0, "p95": 0, "p99": 0, "samples": 0}
            sorted_samples = sorted(self._latency_samples)
            n = len(sorted_samples)
            return {
                "p50": sorted_samples[int(n * 0.5)],
                "p95": sorted_samples[int(n * 0.95)] if n > 1 else sorted_samples[0],
                "p99": sorted_samples[min(int(n * 0.99), n - 1)],
                "samples": n,
            }

    @property
    def throughput_history(self) -> list:
        """Last 60 seconds of throughput as [{"time": epoch, "mps": float}, ...]"""
        with self._lock:
            return list(self._throughput_history)

    def _snapshot_throughput(self):
        """Compute messages in the last 1 second and append to history."""
        now = time.time()
        cutoff = now - 1.0
        with self._lock:
            recent = sum(1 for t in self._message_timestamps if t > cutoff)
            self._throughput_history.append({"time": now, "mps": float(recent)})

    def _throughput_snapshot_loop(self):
        """Background loop that records throughput snapshots every second."""
        while self._running:
            self._snapshot_throughput()
            time.sleep(1.0)
