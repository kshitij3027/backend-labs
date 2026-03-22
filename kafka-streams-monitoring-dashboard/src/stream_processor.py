"""Route incoming Kafka messages to the metrics store by topic."""

import logging

logger = logging.getLogger(__name__)


class StreamProcessor:
    """Dispatches messages to topic-specific handlers."""

    def __init__(self, metrics_store, business_metrics=None, geo_analyzer=None):
        self._metrics_store = metrics_store
        self._business_metrics = business_metrics
        self._geo_analyzer = geo_analyzer
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

        if self._business_metrics:
            self._business_metrics.track_api_version(data.get("path"))

        if self._geo_analyzer:
            ip = data.get("ip_address")
            if ip:
                self._geo_analyzer.analyze_ip(ip)
                rt = data.get("response_time")
                if rt is not None:
                    self._geo_analyzer.record_latency(ip, rt)

    def _handle_error_event(self, data):
        self._metrics_store.add_event("error-events", data)

    def _handle_user_event(self, data):
        self._metrics_store.add_event("user-events", data)

        if self._business_metrics:
            action = data.get("action", "")
            path = data.get("path", "")
            self._business_metrics.track_payment_funnel(path, action)

            if action in ("login", "signup"):
                # Treat all generated events as successful for demo
                self._business_metrics.track_auth_event(action, success=True)
