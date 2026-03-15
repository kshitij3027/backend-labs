"""Dashboard consumer — reads all service topics for the real-time dashboard."""

import json
import threading
import time
from collections import defaultdict, deque

import structlog
from confluent_kafka import Consumer, KafkaError

from src.config import Settings
from src.models import LogMessage, LogLevel, ServiceName

logger = structlog.get_logger()


class DashboardConsumer:
    """Consumes from all service topics for the real-time dashboard.

    Runs in a background daemon thread. Maintains a bounded deque of recent
    messages and per-service/per-level counters. Thread-safe reads.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._recent_messages: deque = deque(maxlen=settings.sse_max_buffer)
        self._stats: dict = {
            "total": 0,
            "by_service": defaultdict(int),
            "by_level": defaultdict(int),
        }
        self._running = False
        self._thread: threading.Thread | None = None
        self._consumer: Consumer | None = None

    def start(self):
        """Start consuming in a background daemon thread."""
        self._running = True
        self._consumer = Consumer({
            "bootstrap.servers": self.settings.active_bootstrap_servers,
            "group.id": self.settings.dashboard_group_id,
            "auto.offset.reset": self.settings.consumer_auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        })
        self._consumer.subscribe(self.settings.all_service_topics)
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info("dashboard_consumer_started", group=self.settings.dashboard_group_id)

    def _consume_loop(self):
        """Poll Kafka in a loop, parse messages, and update shared state."""
        while self._running:
            try:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("consumer_error", error=str(msg.error()))
                    continue

                try:
                    log_msg = LogMessage.from_kafka_value(msg.value())
                    message_dict = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "data": log_msg.model_dump(),
                        "received_at": time.time(),
                    }

                    with self._lock:
                        self._recent_messages.append(message_dict)
                        self._stats["total"] += 1
                        self._stats["by_service"][log_msg.service.value] += 1
                        self._stats["by_level"][log_msg.level.value] += 1
                except Exception as e:
                    logger.warning("message_parse_error", error=str(e))
            except Exception as e:
                logger.error("consume_loop_error", error=str(e))
                if self._running:
                    time.sleep(1)

    def stop(self):
        """Signal the consume loop to stop, join the thread, and close the consumer."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        if self._consumer:
            self._consumer.close()
        logger.info("dashboard_consumer_stopped")

    @property
    def recent_messages(self) -> list[dict]:
        """Return a snapshot copy of the recent messages buffer."""
        with self._lock:
            return list(self._recent_messages)

    @property
    def stats(self) -> dict:
        """Return a snapshot copy of the current statistics."""
        with self._lock:
            return {
                "total": self._stats["total"],
                "by_service": dict(self._stats["by_service"]),
                "by_level": dict(self._stats["by_level"]),
            }

    @property
    def is_running(self) -> bool:
        """Return True if the background thread is alive and the consumer is active."""
        return self._running and self._thread is not None and self._thread.is_alive()
