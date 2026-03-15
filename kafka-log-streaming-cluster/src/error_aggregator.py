"""Error aggregator consumer — filters ERROR-level logs and tracks error metrics."""

import threading
import time
from collections import defaultdict, deque

import structlog
from confluent_kafka import Consumer, KafkaError

from src.config import Settings
from src.models import LogLevel, LogMessage

logger = structlog.get_logger()

# Rolling window size (in seconds) for error-rate calculation.
_ERROR_RATE_WINDOW = 60


class ErrorAggregator:
    """Consumes all service topics but only retains ERROR-level messages.

    Tracks per-service error counts and a rolling error-rate (errors/second)
    over the last 60 seconds.  Thread-safe reads via a shared lock.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = threading.Lock()
        self._recent_errors: deque = deque(maxlen=settings.sse_max_buffer)
        self._error_counts: defaultdict[str, int] = defaultdict(int)
        self._error_timestamps: deque[float] = deque(maxlen=10000)
        self._running = False
        self._thread: threading.Thread | None = None
        self._consumer: Consumer | None = None

    def start(self):
        """Start consuming in a background daemon thread."""
        self._running = True
        self._consumer = Consumer({
            "bootstrap.servers": self.settings.active_bootstrap_servers,
            "group.id": self.settings.error_aggregator_group_id,
            "auto.offset.reset": self.settings.consumer_auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        })
        self._consumer.subscribe(self.settings.all_service_topics)
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()
        logger.info(
            "error_aggregator_started",
            group=self.settings.error_aggregator_group_id,
        )

    def _consume_loop(self):
        """Poll Kafka, parse messages, and keep only ERROR-level entries."""
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

                    # Only keep ERROR-level messages.
                    if log_msg.level != LogLevel.ERROR:
                        continue

                    error_dict = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "data": log_msg.model_dump(),
                        "received_at": time.time(),
                    }

                    with self._lock:
                        self._recent_errors.append(error_dict)
                        self._error_counts[log_msg.service.value] += 1
                        self._error_timestamps.append(time.time())
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
        logger.info("error_aggregator_stopped")

    @property
    def recent_errors(self) -> list[dict]:
        """Return a snapshot copy of the recent errors buffer."""
        with self._lock:
            return list(self._recent_errors)

    @property
    def error_counts(self) -> dict[str, int]:
        """Return per-service error counts."""
        with self._lock:
            return dict(self._error_counts)

    @property
    def error_rate(self) -> float:
        """Return errors per second over the last 60 seconds."""
        with self._lock:
            now = time.time()
            cutoff = now - _ERROR_RATE_WINDOW
            recent = sum(1 for t in self._error_timestamps if t > cutoff)
            return round(recent / _ERROR_RATE_WINDOW, 2)

    @property
    def is_running(self) -> bool:
        """Return True if the background thread is alive and the consumer is active."""
        return self._running and self._thread is not None and self._thread.is_alive()
