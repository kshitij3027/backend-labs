"""Kafka consumer with batch processing and manual offset commits."""
import logging
import threading
import time
from typing import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.config import Settings

logger = logging.getLogger(__name__)


class LogConsumer:
    """Background Kafka consumer thread with batch accumulation and manual commits."""

    def __init__(
        self,
        settings: Settings,
        on_batch: Callable[[list], None] | None = None,
    ) -> None:
        self._settings = settings
        self._on_batch = on_batch
        self._consumer: Consumer | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._assigned_partitions: list[TopicPartition] = []
        self._batch: list = []
        self._batch_start: float = 0.0
        self._last_batch_duration: float = 0.0

        # Stats
        self._total_consumed = 0
        self._total_committed = 0
        self._total_errors = 0
        self._batches_processed = 0
        self._start_time: float = 0.0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the consumer in a background daemon thread."""
        if self._running:
            return

        conf = {
            "bootstrap.servers": self._settings.bootstrap_servers,
            "group.id": self._settings.group_id,
            "auto.offset.reset": self._settings.auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": self._settings.session_timeout_ms,
            "heartbeat.interval.ms": self._settings.heartbeat_interval_ms,
            "max.poll.interval.ms": 300000,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe(
            self._settings.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self._running = True
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Consumer started — group=%s, topics=%s",
            self._settings.group_id,
            self._settings.topics,
        )

    def stop(self) -> None:
        """Signal the consumer loop to stop and wait for the thread."""
        if not self._running:
            return
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=10)
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
        logger.info("Consumer stopped — total_consumed=%d", self._total_consumed)

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Rebalance callbacks
    # ------------------------------------------------------------------

    def _on_assign(self, consumer, partitions) -> None:
        """Called when partitions are assigned."""
        self._assigned_partitions = partitions
        logger.info("Partitions assigned: %s", [str(p) for p in partitions])

    def _on_revoke(self, consumer, partitions) -> None:
        """Called when partitions are revoked — flush partial batch."""
        if self._batch:
            logger.info("Flushing %d messages before revoke", len(self._batch))
            try:
                self._process_batch()
            except Exception as exc:
                logger.error("Failed to flush batch during revoke: %s", exc)
                self._batch = []
        self._assigned_partitions = []
        logger.info("Partitions revoked: %s", [str(p) for p in partitions])

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    def _consume_loop(self) -> None:
        """Main poll → batch → process → commit loop."""
        self._batch_start = time.time()

        while self._running:
            try:
                msg = self._consumer.poll(timeout=self._settings.poll_timeout_s)

                if msg is None:
                    # Check batch timeout
                    if self._batch and (time.time() - self._batch_start) >= self._settings.batch_timeout_s:
                        self._process_with_retry()
                        # Dynamic throttling
                        if self._last_batch_duration > 0.5:
                            throttle = min(self._last_batch_duration * 0.5, 2.0)
                            logger.debug("Throttling: sleeping %.2fs (batch took %.3fs)", throttle, self._last_batch_duration)
                            time.sleep(throttle)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    with self._lock:
                        self._total_errors += 1
                    continue

                self._batch.append(msg)

                # Process when batch is full
                if len(self._batch) >= self._settings.batch_size:
                    self._process_with_retry()
                    # Dynamic throttling
                    if self._last_batch_duration > 0.5:
                        throttle = min(self._last_batch_duration * 0.5, 2.0)
                        logger.debug("Throttling: sleeping %.2fs (batch took %.3fs)", throttle, self._last_batch_duration)
                        time.sleep(throttle)

            except KafkaException as exc:
                logger.error("Kafka exception: %s", exc)
                with self._lock:
                    self._total_errors += 1
            except Exception as exc:
                logger.error("Unexpected error in consume loop: %s", exc)
                with self._lock:
                    self._total_errors += 1

        # Final flush on shutdown
        if self._batch:
            self._process_with_retry()

    def _process_with_retry(self) -> None:
        """Process batch with exponential backoff retry."""
        max_retries = 3
        delays = [1, 2, 4]  # seconds

        for attempt in range(max_retries + 1):
            try:
                self._process_batch()
                return
            except Exception as exc:
                if attempt < max_retries:
                    delay = delays[attempt]
                    logger.warning(
                        "Batch processing failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1, max_retries, delay, exc,
                    )
                    time.sleep(delay)
                else:
                    logger.error("Batch processing failed after %d retries: %s", max_retries, exc)
                    self._send_to_dead_letter(self._batch)
                    self._batch = []
                    self._batch_start = time.time()

    def _process_batch(self) -> None:
        """Process the current batch and commit offsets."""
        if not self._batch:
            return

        batch = self._batch
        self._batch = []
        batch_size = len(batch)
        batch_start = time.time()

        try:
            if self._on_batch:
                self._on_batch(batch)

            # Commit offsets
            self._consumer.commit(asynchronous=False)

            with self._lock:
                self._total_consumed += batch_size
                self._total_committed += batch_size
                self._batches_processed += 1

            logger.info(
                "Batch processed — size=%d, total=%d",
                batch_size,
                self._total_consumed,
            )
        except Exception as exc:
            logger.error("Batch processing failed: %s", exc)
            # Restore the batch so _process_with_retry can retry or send to DLQ
            self._batch = batch
            # Re-raise so _process_with_retry can handle retries / DLQ
            raise

        self._last_batch_duration = time.time() - batch_start
        self._batch_start = time.time()

    def _send_to_dead_letter(self, messages: list) -> None:
        """Send failed messages to dead-letter-logs topic."""
        try:
            from confluent_kafka import Producer
            producer = Producer({"bootstrap.servers": self._settings.bootstrap_servers})
            for msg in messages:
                try:
                    value = msg.value() if hasattr(msg, "value") else str(msg).encode()
                    producer.produce(
                        topic="dead-letter-logs",
                        value=value,
                        headers={
                            "error": "processing_failed",
                            "original_topic": msg.topic() if hasattr(msg, "topic") else "unknown",
                        },
                    )
                except Exception:
                    pass
            producer.flush(timeout=5)
            logger.info("Sent %d messages to dead-letter-logs", len(messages))
        except Exception as exc:
            logger.error("Failed to send to dead letter topic: %s", exc)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        """Thread-safe snapshot of consumer statistics."""
        with self._lock:
            elapsed = time.time() - self._start_time if self._start_time else 0
            return {
                "is_running": self._running,
                "total_consumed": self._total_consumed,
                "total_committed": self._total_committed,
                "total_errors": self._total_errors,
                "batches_processed": self._batches_processed,
                "uptime_seconds": round(elapsed, 1),
                "throughput": round(self._total_consumed / elapsed, 2) if elapsed > 0 else 0,
                "assigned_partitions": len(self._assigned_partitions),
                "current_batch_size": len(self._batch),
            }

    @property
    def assigned_partitions(self) -> list:
        return list(self._assigned_partitions)
