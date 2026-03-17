"""Application configuration loaded from environment variables."""
import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    """All configurable parameters for the consumer application."""

    # Kafka
    bootstrap_servers: str = "kafka:29092"
    group_id: str = "log-processing-group"
    topics: list[str] = field(default_factory=lambda: ["web-logs", "app-logs", "error-logs"])
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 45000
    heartbeat_interval_ms: int = 15000
    max_poll_records: int = 500

    # Batch processing
    batch_size: int = 100
    batch_timeout_s: float = 5.0
    poll_timeout_s: float = 1.0

    # Redis
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8080
    ws_broadcast_interval: float = 1.0

    # Analytics
    sliding_window_seconds: int = 60

    # Persistence
    snapshot_interval_s: float = 10.0


def load_config() -> Settings:
    """Create Settings from environment variables.

    Env vars use uppercase names matching the field names.
    List fields (topics) are comma-separated.
    """
    kwargs = {}

    env_map = {
        "KAFKA_BOOTSTRAP_SERVERS": ("bootstrap_servers", str),
        "KAFKA_CONSUMER_GROUP": ("group_id", str),
        "KAFKA_TOPICS": ("topics", lambda v: [t.strip() for t in v.split(",")]),
        "KAFKA_AUTO_OFFSET_RESET": ("auto_offset_reset", str),
        "KAFKA_SESSION_TIMEOUT_MS": ("session_timeout_ms", int),
        "KAFKA_HEARTBEAT_INTERVAL_MS": ("heartbeat_interval_ms", int),
        "KAFKA_MAX_POLL_RECORDS": ("max_poll_records", int),
        "BATCH_SIZE": ("batch_size", int),
        "BATCH_TIMEOUT_S": ("batch_timeout_s", float),
        "POLL_TIMEOUT_S": ("poll_timeout_s", float),
        "REDIS_HOST": ("redis_host", str),
        "REDIS_PORT": ("redis_port", int),
        "REDIS_DB": ("redis_db", int),
        "DASHBOARD_HOST": ("dashboard_host", str),
        "DASHBOARD_PORT": ("dashboard_port", int),
        "WS_BROADCAST_INTERVAL": ("ws_broadcast_interval", float),
        "SLIDING_WINDOW_SECONDS": ("sliding_window_seconds", int),
        "SNAPSHOT_INTERVAL_S": ("snapshot_interval_s", float),
    }

    for env_var, (field_name, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            kwargs[field_name] = converter(value)

    return Settings(**kwargs)
