"""Processes consumed Kafka messages."""
import logging
from src.models import LogEntry
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class LogProcessor:
    """Deserializes and processes log entries from Kafka messages."""

    def __init__(self, consumer_id: str, metrics: MetricsCollector) -> None:
        self._consumer_id = consumer_id
        self._metrics = metrics

    def process(self, msg) -> LogEntry | None:
        """Process a single Kafka message. Returns LogEntry or None on error."""
        try:
            entry = LogEntry.from_kafka_value(msg.value())
            self._metrics.record_consumed(self._consumer_id, msg.partition())
            return entry
        except Exception as e:
            logger.error("Consumer %s failed to process message: %s", self._consumer_id, e)
            self._metrics.record_error(self._consumer_id)
            return None
