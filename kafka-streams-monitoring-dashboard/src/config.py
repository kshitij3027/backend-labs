"""Application configuration with environment variable overrides."""

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    """Central configuration for the Kafka streams monitoring dashboard."""

    # Kafka connection
    bootstrap_servers: str = "kafka:29092"
    group_id: str = "dashboard-consumer"
    topics: list = field(default_factory=lambda: ["log-events", "error-events", "user-events"])
    auto_offset_reset: str = "earliest"

    # Stream processing
    window_seconds: int = 60
    deque_max_length: int = 1000

    # WebSocket
    ws_emit_interval: float = 2.0

    # Consumer
    poll_timeout_s: float = 1.0

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 5000

    # Derived metrics
    derived_metrics_topic: str = "derived-metrics"

    # Alert thresholds
    alert_error_rate_warning: float = 3.0
    alert_error_rate_critical: float = 5.0
    alert_response_time_warning: float = 1000.0
    alert_response_time_critical: float = 2000.0
    alert_cooldown_seconds: float = 60.0


def load_config() -> Settings:
    """Load settings, overriding defaults with environment variables where set."""
    kwargs: dict = {}
    env_map = {
        "KAFKA_BOOTSTRAP_SERVERS": ("bootstrap_servers", str),
        "KAFKA_GROUP_ID": ("group_id", str),
        "AUTO_OFFSET_RESET": ("auto_offset_reset", str),
        "WINDOW_SECONDS": ("window_seconds", int),
        "DEQUE_MAX_LENGTH": ("deque_max_length", int),
        "WS_EMIT_INTERVAL": ("ws_emit_interval", float),
        "POLL_TIMEOUT_S": ("poll_timeout_s", float),
        "DASHBOARD_HOST": ("dashboard_host", str),
        "DASHBOARD_PORT": ("dashboard_port", int),
        "ALERT_ERROR_RATE_WARNING": ("alert_error_rate_warning", float),
        "ALERT_ERROR_RATE_CRITICAL": ("alert_error_rate_critical", float),
        "ALERT_RESPONSE_TIME_WARNING": ("alert_response_time_warning", float),
        "ALERT_RESPONSE_TIME_CRITICAL": ("alert_response_time_critical", float),
        "ALERT_COOLDOWN_SECONDS": ("alert_cooldown_seconds", float),
    }

    for env_var, (field_name, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                kwargs[field_name] = converter(value)
            except (ValueError, TypeError):
                pass  # Fall back to default if conversion fails

    return Settings(**kwargs)
