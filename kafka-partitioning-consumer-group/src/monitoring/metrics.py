"""Thread-safe metrics collector for consumer group monitoring."""
import threading
import time
from collections import deque


class MetricsCollector:
    """Collects and aggregates consumer group metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._start_time = time.time()
        self._total_consumed = 0
        self._total_errors = 0
        self._per_consumer: dict[str, dict] = {}
        self._per_partition: dict[int, int] = {}
        self._throughput_window: deque = deque(maxlen=60)
        self._rebalance_events: list[dict] = []
        self._lag: dict[int, int] = {}
        self._scaling_events: list[dict] = []

    def record_consumed(self, consumer_id: str, partition: int, count: int = 1) -> None:
        """Record that a consumer processed messages from a partition."""
        with self._lock:
            self._total_consumed += count
            self._per_partition[partition] = self._per_partition.get(partition, 0) + count
            if consumer_id not in self._per_consumer:
                self._per_consumer[consumer_id] = {"consumed": 0, "errors": 0, "partitions": set()}
            self._per_consumer[consumer_id]["consumed"] += count
            self._per_consumer[consumer_id]["partitions"].add(partition)

    def record_error(self, consumer_id: str, count: int = 1) -> None:
        """Record processing errors."""
        with self._lock:
            self._total_errors += count
            if consumer_id not in self._per_consumer:
                self._per_consumer[consumer_id] = {"consumed": 0, "errors": 0, "partitions": set()}
            self._per_consumer[consumer_id]["errors"] += count

    def record_throughput(self, messages_per_second: float) -> None:
        """Record a throughput sample."""
        with self._lock:
            self._throughput_window.append({
                "timestamp": time.time(),
                "mps": messages_per_second,
            })

    def record_rebalance(self, event_type: str, partitions: list[int], consumer_id: str = "") -> None:
        """Record a rebalance event."""
        with self._lock:
            self._rebalance_events.append({
                "timestamp": time.time(),
                "type": event_type,
                "partitions": partitions,
                "consumer_id": consumer_id,
            })

    def update_lag(self, partition: int, lag: int) -> None:
        """Update consumer lag for a partition."""
        with self._lock:
            self._lag[partition] = lag

    def record_scaling_event(self, event: dict) -> None:
        """Record a scaling event."""
        with self._lock:
            self._scaling_events.append(event)

    def get_lag_history(self) -> list[dict]:
        """Return lag history."""
        with self._lock:
            return [{"timestamp": time.time(), "lag": dict(self._lag)}]

    def snapshot(self) -> dict:
        """Return a point-in-time snapshot of all metrics."""
        with self._lock:
            per_consumer = {}
            for cid, data in self._per_consumer.items():
                per_consumer[cid] = {
                    "consumed": data["consumed"],
                    "errors": data["errors"],
                    "partitions": sorted(data["partitions"]),
                }

            return {
                "total_consumed": self._total_consumed,
                "total_errors": self._total_errors,
                "uptime_seconds": round(time.time() - self._start_time, 1),
                "throughput": list(self._throughput_window),
                "per_consumer": per_consumer,
                "per_partition": dict(self._per_partition),
                "rebalance_events": list(self._rebalance_events),
                "lag": dict(self._lag),
                "scaling_events": list(self._scaling_events),
            }
