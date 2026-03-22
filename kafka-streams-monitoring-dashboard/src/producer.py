"""Kafka producer for publishing derived/aggregated metrics back to Kafka."""

import json
import logging
import time

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class MetricsProducer:
    """Produces windowed aggregated metrics to the derived-metrics Kafka topic."""

    def __init__(self, config):
        self._config = config
        self._producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "acks": "all",
        })
        self._topic = config.derived_metrics_topic
        logger.info("MetricsProducer initialized for topic: %s", self._topic)

    def _delivery_callback(self, err, msg):
        """Log delivery success or failure."""
        if err is not None:
            logger.error("Derived metrics delivery failed: %s", err)
        else:
            logger.debug(
                "Derived metrics delivered to %s [%d]",
                msg.topic(),
                msg.partition(),
            )

    def produce_derived_metrics(self, metrics_dict):
        """Produce aggregated metrics to the derived-metrics topic."""
        try:
            now = time.time()
            window_seconds = metrics_dict.get("window_seconds", 60)
            payload = {
                **metrics_dict,
                "window_start": now - window_seconds,
                "window_end": now,
                "produced_at": now,
            }
            self._producer.produce(
                topic=self._topic,
                key="windowed-metrics",
                value=json.dumps(payload).encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        except Exception as e:
            logger.error("Failed to produce derived metrics: %s", e)

    def flush(self):
        """Flush pending messages."""
        self._producer.flush(timeout=5)

    def close(self):
        """Flush and shut down the producer."""
        self.flush()
        logger.info("MetricsProducer closed")
