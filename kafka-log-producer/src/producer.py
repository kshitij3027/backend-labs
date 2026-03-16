"""High-performance Kafka log producer with delivery tracking."""

import time
import threading
from collections import defaultdict

from confluent_kafka import Producer, KafkaException

from src.config import Config
from src.fallback_storage import FallbackStorage
from src.metrics import ProducerMetrics
from src.models import LogEntry


class KafkaLogProducer:
    """Thread-safe Kafka producer that routes LogEntry instances to topics
    and tracks delivery statistics via asynchronous callbacks."""

    def __init__(self, config: Config) -> None:
        self._producer = Producer(config.kafka_config)
        self._config = config

        # Thread-safe counters
        self._lock = threading.Lock()
        self._sent: int = 0
        self._failed: int = 0
        self._topic_counts: dict[str, int] = defaultdict(int)
        self._partition_counts: dict[str, int] = defaultdict(int)

        self._metrics = ProducerMetrics()
        self._fallback = FallbackStorage(config.fallback_path)

        self._closed = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_log(self, entry: LogEntry) -> None:
        """Produce a single log entry to its routed Kafka topic."""
        if self._closed:
            raise RuntimeError("Producer is closed")

        topic = entry.route_topic()
        key = entry.to_kafka_key()
        value = entry.to_kafka_value()
        start = time.time()

        def _on_delivery(err, msg):
            latency = time.time() - start
            self._delivery_callback(err, msg, topic=topic, latency=latency)

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=_on_delivery,
            )
            # Trigger any pending callbacks without blocking
            self._producer.poll(0)
        except (BufferError, KafkaException) as exc:
            self._fallback.write([entry])
            self._metrics.record_failure(topic, type(exc).__name__)
            with self._lock:
                self._failed += 1

    def send_logs_batch(self, entries: list[LogEntry]) -> dict[str, int]:
        """Send a batch of log entries and flush, returning delivery counts."""
        for entry in entries:
            self.send_log(entry)

        self.flush()

        with self._lock:
            return {"sent": self._sent, "failed": self._failed}

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all buffered messages. Returns the number still in queue."""
        return self._producer.flush(timeout)

    def close(self) -> None:
        """Flush remaining messages and mark the producer as closed."""
        self.flush()
        self._closed = True

    # ------------------------------------------------------------------
    # Delivery callback
    # ------------------------------------------------------------------

    def _delivery_callback(
        self, err, msg, *, topic: str = "", latency: float = 0.0,
    ) -> None:
        """Called once per message by librdkafka's background thread."""
        resolved_topic = topic or msg.topic()
        with self._lock:
            if err is not None:
                self._failed += 1
                self._metrics.record_failure(resolved_topic, str(err))
            else:
                self._sent += 1
                self._topic_counts[msg.topic()] += 1
                partition_key = f"{msg.topic()}-{msg.partition()}"
                self._partition_counts[partition_key] += 1
                self._metrics.record_success(resolved_topic, latency)

    # ------------------------------------------------------------------
    # Fallback replay
    # ------------------------------------------------------------------

    def _try_replay_fallback(self) -> None:
        """If the fallback file has data, drain it by re-sending entries."""
        if not self._fallback.has_data():
            return

        def _replay(entries: list[LogEntry]) -> None:
            for entry in entries:
                self.send_log(entry)

        self._fallback.drain(_replay)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        """Return a thread-safe snapshot of delivery statistics."""
        with self._lock:
            total = self._sent + self._failed
            success_rate = (self._sent / total * 100.0) if total > 0 else 0.0
            base = {
                "total_sent": self._sent,
                "total_failed": self._failed,
                "topic_counts": dict(self._topic_counts),
                "partition_counts": dict(self._partition_counts),
                "success_rate": success_rate,
            }
        base["metrics"] = self._metrics.snapshot
        return base

    @property
    def metrics(self) -> ProducerMetrics:
        """Expose the metrics collector for external access."""
        return self._metrics

    def reset_stats(self) -> None:
        """Reset all delivery counters to zero."""
        with self._lock:
            self._sent = 0
            self._failed = 0
            self._topic_counts.clear()
            self._partition_counts.clear()
