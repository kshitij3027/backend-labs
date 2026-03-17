"""Batch processor for parsing and routing Kafka messages."""
import logging
import threading
import time

from src.models import parse_log_message, WebAccessLog, AppLog, ErrorLog, LogMessage

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Deserializes Kafka messages, routes by log type, tracks stats."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_processed = 0
        self._total_failed = 0
        self._web_count = 0
        self._app_count = 0
        self._error_count = 0

    def process_batch(self, messages: list) -> list[LogMessage]:
        """Parse a batch of raw Kafka messages into typed log models.

        Returns the list of successfully parsed LogMessage objects.
        """
        parsed: list[LogMessage] = []
        batch_start = time.time()

        for msg in messages:
            try:
                topic = msg.topic() if hasattr(msg, "topic") else ""
                value = msg.value() if hasattr(msg, "value") else msg
                if isinstance(value, bytes):
                    log = parse_log_message(value, topic=topic)
                else:
                    log = None

                if log is None:
                    with self._lock:
                        self._total_failed += 1
                    continue

                parsed.append(log)

                with self._lock:
                    self._total_processed += 1
                    if isinstance(log, WebAccessLog):
                        self._web_count += 1
                    elif isinstance(log, ErrorLog):
                        self._error_count += 1
                    elif isinstance(log, AppLog):
                        self._app_count += 1

            except Exception as exc:
                logger.error("Failed to parse message: %s", exc)
                with self._lock:
                    self._total_failed += 1

        elapsed = time.time() - batch_start
        logger.debug(
            "Batch parsed — %d/%d ok, %.3fs",
            len(parsed),
            len(messages),
            elapsed,
        )
        return parsed

    @property
    def stats(self) -> dict:
        """Thread-safe snapshot of processing statistics."""
        with self._lock:
            total = self._total_processed + self._total_failed
            return {
                "total_processed": self._total_processed,
                "total_failed": self._total_failed,
                "success_rate": round(
                    self._total_processed / total * 100, 2
                ) if total > 0 else 100.0,
                "web_count": self._web_count,
                "app_count": self._app_count,
                "error_count": self._error_count,
            }
