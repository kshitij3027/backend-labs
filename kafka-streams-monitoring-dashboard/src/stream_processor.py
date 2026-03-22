"""Route incoming Kafka messages to the metrics store by topic."""

import logging

logger = logging.getLogger(__name__)


class StreamProcessor:
    """Dispatches messages to topic-specific handlers."""

    def __init__(self, metrics_store):
        self._metrics_store = metrics_store
        self._handlers = {
            "log-events": self._handle_log_event,
            "error-events": self._handle_error_event,
            "user-events": self._handle_user_event,
        }

    def process_message(self, topic, key, value):
        """Route message to the appropriate handler based on topic."""
        handler = self._handlers.get(topic)
        if handler is None:
            logger.warning("Unknown topic: %s", topic)
            return

        try:
            handler(value)
        except Exception as e:
            logger.error("Error processing message from %s: %s", topic, e)

    def _handle_log_event(self, data):
        self._metrics_store.add_event("log-events", data)

    def _handle_error_event(self, data):
        self._metrics_store.add_event("error-events", data)

    def _handle_user_event(self, data):
        self._metrics_store.add_event("user-events", data)
