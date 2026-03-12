"""Redelivery handler managing retry queues, DLQ, and message republishing."""

from typing import Any

import pika

from src.config import Settings
from src.logging_config import get_logger

logger = get_logger(__name__)


class RedeliveryHandler:
    """Manages RabbitMQ retry/DLQ infrastructure and message redelivery logic.

    Uses per-delay retry queues with TTL-based dead-letter routing so that
    messages automatically flow back to the main queue after each delay.
    """

    def __init__(self, config: Settings) -> None:
        self.config = config

    def declare_infrastructure(self, channel: Any) -> None:
        """Declare all exchanges, queues, and bindings for the retry pipeline.

        Creates:
        - Main exchange (direct) and main queue bound to it
        - Retry exchange (direct) and per-delay retry queues
        - DLQ exchange (direct) and DLQ queue
        """
        # --- Main exchange + queue ---
        channel.exchange_declare(
            exchange=self.config.MAIN_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue=self.config.MAIN_QUEUE,
            durable=True,
        )
        channel.queue_bind(
            queue=self.config.MAIN_QUEUE,
            exchange=self.config.MAIN_EXCHANGE,
            routing_key=self.config.MAIN_QUEUE,
        )
        logger.info(
            "main_infrastructure_declared",
            exchange=self.config.MAIN_EXCHANGE,
            queue=self.config.MAIN_QUEUE,
        )

        # --- Retry exchange + per-delay queues ---
        channel.exchange_declare(
            exchange=self.config.RETRY_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        for delay in self.config.RETRY_DELAYS:
            retry_queue_name = f"logs.retry.{delay}ms"
            channel.queue_declare(
                queue=retry_queue_name,
                durable=True,
                arguments={
                    "x-message-ttl": delay,
                    "x-dead-letter-exchange": self.config.MAIN_EXCHANGE,
                    "x-dead-letter-routing-key": self.config.MAIN_QUEUE,
                },
            )
            channel.queue_bind(
                queue=retry_queue_name,
                exchange=self.config.RETRY_EXCHANGE,
                routing_key=retry_queue_name,
            )
            logger.info(
                "retry_queue_declared",
                queue=retry_queue_name,
                ttl_ms=delay,
            )

        # --- DLQ exchange + queue ---
        channel.exchange_declare(
            exchange=self.config.DLQ_EXCHANGE,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue=self.config.DLQ_QUEUE,
            durable=True,
        )
        channel.queue_bind(
            queue=self.config.DLQ_QUEUE,
            exchange=self.config.DLQ_EXCHANGE,
            routing_key=self.config.DLQ_QUEUE,
        )
        logger.info(
            "dlq_infrastructure_declared",
            exchange=self.config.DLQ_EXCHANGE,
            queue=self.config.DLQ_QUEUE,
        )

    def get_retry_count(self, properties: Any) -> int:
        """Extract the retry count from message properties.

        Looks for `x-retry-count` in properties.headers.
        Returns 0 if no header is present.
        """
        if properties.headers and "x-retry-count" in properties.headers:
            return int(properties.headers["x-retry-count"])
        return 0

    def should_retry(self, retry_count: int) -> bool:
        """Return True if the message has not exceeded the maximum retry limit."""
        return retry_count < self.config.MAX_RETRIES

    def get_retry_queue_name(self, retry_count: int) -> str:
        """Return the retry queue name for the given retry attempt.

        Maps retry_count to an index in RETRY_DELAYS, capping at the
        last available delay for counts beyond the list length.
        """
        delays = self.config.RETRY_DELAYS
        index = min(retry_count, len(delays) - 1)
        delay = delays[index]
        return f"logs.retry.{delay}ms"

    def publish_to_retry(
        self,
        channel: Any,
        body: bytes,
        properties: Any,
        retry_count: int,
    ) -> None:
        """Publish a message to the appropriate retry queue.

        Updates the x-retry-count header before publishing.
        """
        retry_queue = self.get_retry_queue_name(retry_count)

        headers = dict(properties.headers) if properties.headers else {}
        headers["x-retry-count"] = retry_count + 1

        retry_properties = pika.BasicProperties(
            delivery_mode=2,  # persistent
            headers=headers,
            content_type=getattr(properties, "content_type", "application/json"),
        )

        channel.basic_publish(
            exchange=self.config.RETRY_EXCHANGE,
            routing_key=retry_queue,
            body=body,
            properties=retry_properties,
        )
        logger.info(
            "published_to_retry",
            retry_queue=retry_queue,
            retry_count=retry_count + 1,
        )

    def publish_to_dlq(
        self,
        channel: Any,
        body: bytes,
        properties: Any,
        error: str,
    ) -> None:
        """Publish a message to the dead-letter queue with error metadata."""
        headers = dict(properties.headers) if properties.headers else {}
        headers["x-dlq-reason"] = error
        headers["x-original-exchange"] = self.config.MAIN_EXCHANGE
        headers["x-original-routing-key"] = self.config.MAIN_QUEUE

        dlq_properties = pika.BasicProperties(
            delivery_mode=2,  # persistent
            headers=headers,
            content_type=getattr(properties, "content_type", "application/json"),
        )

        channel.basic_publish(
            exchange=self.config.DLQ_EXCHANGE,
            routing_key=self.config.DLQ_QUEUE,
            body=body,
            properties=dlq_properties,
        )
        logger.info(
            "published_to_dlq",
            error=error,
        )
