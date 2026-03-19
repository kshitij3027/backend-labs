"""Application configuration loaded from environment variables."""
import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    """All configurable parameters for the Kafka partitioning consumer group."""

    # Kafka
    bootstrap_servers: str = "kafka:29092"
    topic: str = "log-processing-topic"
    num_partitions: int = 6
    group_id: str = "log-processing-group"

    # Consumer
    num_consumers: int = 3
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000

    # Producer
    producer_rate: int = 20
    duration: int = 60
    partition_strategy: str = "key-based"

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8080

    # Log generation
    services: list[str] = field(default_factory=lambda: [
        "auth-service", "api-gateway", "user-service",
        "payment-service", "notification-service", "search-service"
    ])
    user_id_min: int = 1000
    user_id_max: int = 9999
    log_level_weights: dict[str, float] = field(default_factory=lambda: {
        "INFO": 0.70,
        "WARNING": 0.20,
        "ERROR": 0.10,
    })

    # Auto-scaling (used in later commits)
    auto_scale_enabled: bool = False
    lag_threshold: int = 1000
    scale_cooldown_s: int = 30
    max_consumers: int = 6


def load_config() -> Settings:
    """Create Settings from environment variables."""
    kwargs = {}

    env_map = {
        "KAFKA_BOOTSTRAP_SERVERS": ("bootstrap_servers", str),
        "KAFKA_TOPIC": ("topic", str),
        "KAFKA_NUM_PARTITIONS": ("num_partitions", int),
        "KAFKA_GROUP_ID": ("group_id", str),
        "NUM_CONSUMERS": ("num_consumers", int),
        "SESSION_TIMEOUT_MS": ("session_timeout_ms", int),
        "HEARTBEAT_INTERVAL_MS": ("heartbeat_interval_ms", int),
        "PRODUCER_RATE": ("producer_rate", int),
        "DURATION": ("duration", int),
        "PARTITION_STRATEGY": ("partition_strategy", str),
        "DASHBOARD_HOST": ("dashboard_host", str),
        "DASHBOARD_PORT": ("dashboard_port", int),
        "AUTO_SCALE_ENABLED": ("auto_scale_enabled", lambda v: v.lower() in ("true", "1", "yes")),
        "LAG_THRESHOLD": ("lag_threshold", int),
        "SCALE_COOLDOWN_S": ("scale_cooldown_s", int),
        "MAX_CONSUMERS": ("max_consumers", int),
    }

    for env_var, (field_name, converter) in env_map.items():
        value = os.environ.get(env_var)
        if value is not None:
            kwargs[field_name] = converter(value)

    return Settings(**kwargs)
