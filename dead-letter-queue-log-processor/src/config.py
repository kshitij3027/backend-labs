"""Configuration for the Dead Letter Queue Log Processor."""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """All configurable parameters, loaded from environment variables."""

    # Redis connection
    redis_url: str = "redis://localhost:6379"

    # Queue / key names
    main_queue: str = "log_processing"
    dlq_queue: str = "dead_letter_queue"
    processed_store: str = "processed_logs"
    retry_set: str = "retry_schedule"
    stats_hash: str = "processing_stats"
    failure_history: str = "failure_history"

    # Retry behaviour
    max_retries: int = 3
    backoff_base: float = 1.0

    # Producer settings
    producer_rate: float = 10.0
    failure_rate: float = 0.3

    # Dashboard
    dashboard_port: int = 8000

    # Demo
    demo_message_count: int = 100

    # Intervals
    retry_poll_interval: float = 0.5
    ws_broadcast_interval: float = 1.5

    # Alerting
    dlq_alert_threshold: int = 50

    # History limits
    failure_history_max: int = 10000


def load_config() -> Settings:
    """Build a Settings instance from environment variables.

    Each field is read from an uppercase env-var matching its name.
    Missing env-vars fall back to the dataclass default.
    """
    kwargs: dict = {}

    env_map = {
        "redis_url": ("REDIS_URL", str),
        "main_queue": ("MAIN_QUEUE", str),
        "dlq_queue": ("DLQ_QUEUE", str),
        "processed_store": ("PROCESSED_STORE", str),
        "retry_set": ("RETRY_SET", str),
        "stats_hash": ("STATS_HASH", str),
        "failure_history": ("FAILURE_HISTORY", str),
        "max_retries": ("MAX_RETRIES", int),
        "backoff_base": ("BACKOFF_BASE", float),
        "producer_rate": ("PRODUCER_RATE", float),
        "failure_rate": ("FAILURE_RATE", float),
        "dashboard_port": ("DASHBOARD_PORT", int),
        "demo_message_count": ("DEMO_MESSAGE_COUNT", int),
        "retry_poll_interval": ("RETRY_POLL_INTERVAL", float),
        "ws_broadcast_interval": ("WS_BROADCAST_INTERVAL", float),
        "dlq_alert_threshold": ("DLQ_ALERT_THRESHOLD", int),
        "failure_history_max": ("FAILURE_HISTORY_MAX", int),
    }

    for field_name, (env_var, cast) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            kwargs[field_name] = cast(value)

    return Settings(**kwargs)
