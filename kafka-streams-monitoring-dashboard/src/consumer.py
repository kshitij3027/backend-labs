"""Background Kafka consumer that feeds the stream processor."""

import json
import logging
import threading

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class KafkaStreamConsumer:
    """Consume messages from Kafka topics in a background thread."""

    def __init__(self, config, stream_processor):
        self._config = config
        self._processor = stream_processor
        self._running = False
        self._thread = None
        self._consumer = None

    def start(self):
        """Start consuming in a background daemon thread."""
        self._running = True
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("Consumer started for topics: %s", self._config.topics)

    def stop(self):
        """Signal the consumer to stop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        if self._consumer:
            self._consumer.close()
        logger.info("Consumer stopped")

    def _create_consumer(self):
        return Consumer(
            {
                "bootstrap.servers": self._config.bootstrap_servers,
                "group.id": self._config.group_id,
                "auto.offset.reset": self._config.auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

    def _consume_loop(self):
        try:
            self._consumer = self._create_consumer()
            self._consumer.subscribe(self._config.topics)
            logger.info("Subscribed to topics: %s", self._config.topics)

            message_count = 0
            while self._running:
                msg = self._consumer.poll(timeout=self._config.poll_timeout_s)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    key = msg.key().decode("utf-8") if msg.key() else None
                    self._processor.process_message(msg.topic(), key, value)
                    message_count += 1

                    if message_count % 100 == 0:
                        self._consumer.commit(asynchronous=False)
                except json.JSONDecodeError as e:
                    logger.warning("Failed to decode message: %s", e)
                except Exception as e:
                    logger.error("Error processing message: %s", e)

            # Final commit
            if message_count > 0:
                try:
                    self._consumer.commit(asynchronous=False)
                except Exception:
                    pass

        except Exception as e:
            logger.error("Consumer loop error: %s", e)
        finally:
            if self._consumer:
                self._consumer.close()
