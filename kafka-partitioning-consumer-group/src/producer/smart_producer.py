"""Smart Kafka producer with key-based partitioning."""
import logging
import threading
import zlib
from confluent_kafka import Producer, KafkaException
from src.config import Settings
from src.models import LogEntry

logger = logging.getLogger(__name__)


class SmartProducer:
    """Produces log entries to Kafka with key-based or round-robin partitioning."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer = Producer({
            "bootstrap.servers": settings.bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": 100,
        })
        self._topic = settings.topic
        self._num_partitions = settings.num_partitions
        self._strategy = settings.partition_strategy
        self._rr_counter = 0
        self._lock = threading.Lock()
        self._stats = {
            "produced": 0,
            "errors": 0,
            "per_partition": {i: 0 for i in range(settings.num_partitions)},
        }

    def _calculate_partition(self, entry: LogEntry) -> int:
        """Calculate target partition for a log entry."""
        key = entry.partition_key()
        if self._strategy == "key-based" and key:
            return zlib.crc32(key.encode()) % self._num_partitions
        # Round-robin fallback
        with self._lock:
            partition = self._rr_counter % self._num_partitions
            self._rr_counter += 1
        return partition

    def _delivery_callback(self, err, msg):
        """Called by librdkafka when a message is delivered or fails."""
        with self._lock:
            if err:
                self._stats["errors"] += 1
                logger.error("Delivery failed: %s", err)
            else:
                self._stats["produced"] += 1
                partition = msg.partition()
                self._stats["per_partition"][partition] = (
                    self._stats["per_partition"].get(partition, 0) + 1
                )

    def produce(self, entry: LogEntry) -> None:
        """Send a log entry to Kafka."""
        partition = self._calculate_partition(entry)
        key = entry.partition_key()
        self._producer.produce(
            topic=self._topic,
            value=entry.to_kafka_value(),
            key=key.encode() if key else None,
            partition=partition,
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages. Returns number of messages still in queue."""
        return self._producer.flush(timeout)

    @property
    def stats(self) -> dict:
        """Return a copy of current delivery stats."""
        with self._lock:
            return dict(self._stats)
