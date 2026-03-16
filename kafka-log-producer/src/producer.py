"""High-performance Kafka log producer with delivery tracking."""

import threading
from collections import defaultdict

from confluent_kafka import Producer, KafkaException

from src.config import Config
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

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=self._delivery_callback,
        )
        # Trigger any pending callbacks without blocking
        self._producer.poll(0)

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

    def _delivery_callback(self, err, msg) -> None:
        """Called once per message by librdkafka's background thread."""
        with self._lock:
            if err is not None:
                self._failed += 1
            else:
                self._sent += 1
                self._topic_counts[msg.topic()] += 1
                partition_key = f"{msg.topic()}-{msg.partition()}"
                self._partition_counts[partition_key] += 1

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        """Return a thread-safe snapshot of delivery statistics."""
        with self._lock:
            total = self._sent + self._failed
            success_rate = (self._sent / total * 100.0) if total > 0 else 0.0
            return {
                "total_sent": self._sent,
                "total_failed": self._failed,
                "topic_counts": dict(self._topic_counts),
                "partition_counts": dict(self._partition_counts),
                "success_rate": success_rate,
            }

    def reset_stats(self) -> None:
        """Reset all delivery counters to zero."""
        with self._lock:
            self._sent = 0
            self._failed = 0
            self._topic_counts.clear()
            self._partition_counts.clear()
