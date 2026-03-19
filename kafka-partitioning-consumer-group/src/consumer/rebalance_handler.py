"""Handles Kafka consumer group rebalance events."""
import logging
from confluent_kafka import TopicPartition
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class RebalanceHandler:
    """Handles partition assignment and revocation during rebalances."""

    def __init__(self, consumer, consumer_id: str, metrics: MetricsCollector) -> None:
        self._consumer = consumer
        self._consumer_id = consumer_id
        self._metrics = metrics

    def on_assign(self, consumer, partitions: list[TopicPartition]) -> None:
        """Called when partitions are assigned to this consumer."""
        partition_ids = [p.partition for p in partitions]
        logger.info("Consumer %s assigned partitions: %s", self._consumer_id, partition_ids)
        self._metrics.record_rebalance("assign", partition_ids, self._consumer_id)

    def on_revoke(self, consumer, partitions: list[TopicPartition]) -> None:
        """Called when partitions are revoked from this consumer."""
        partition_ids = [p.partition for p in partitions]
        logger.info("Consumer %s revoked partitions: %s", self._consumer_id, partition_ids)
        self._metrics.record_rebalance("revoke", partition_ids, self._consumer_id)
        # Commit offsets before losing partitions
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            logger.warning("Commit during revoke failed for %s: %s", self._consumer_id, e)
