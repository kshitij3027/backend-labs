"""Application configuration with environment variable overrides."""

import os
from dataclasses import dataclass, fields


@dataclass
class Settings:
    """Central configuration for the Kafka log streaming cluster."""

    # Kafka connection
    bootstrap_servers: str = "localhost:9092,localhost:9093,localhost:9094"
    bootstrap_servers_internal: str = "kafka-1:29092,kafka-2:29092,kafka-3:29092"
    use_internal_listeners: bool = False  # Set to True inside Docker

    # Topics
    web_api_topic: str = "web-api-logs"
    user_service_topic: str = "user-service-logs"
    payment_service_topic: str = "payment-service-logs"
    critical_topic: str = "critical-logs"

    # Producer tuning
    producer_batch_size: int = 200000
    producer_linger_ms: int = 100
    producer_compression: str = "lz4"
    producer_duration_seconds: int = 60
    producer_rate_per_second: float = 100.0

    # Consumer
    dashboard_group_id: str = "dashboard-consumer"
    error_aggregator_group_id: str = "error-aggregator-consumer"
    consumer_auto_offset_reset: str = "earliest"

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000
    sse_max_buffer: int = 1000

    @property
    def active_bootstrap_servers(self) -> str:
        """Return internal or external bootstrap servers based on listener mode."""
        return self.bootstrap_servers_internal if self.use_internal_listeners else self.bootstrap_servers

    @property
    def all_service_topics(self) -> list[str]:
        """Return the list of all service log topics."""
        return [self.web_api_topic, self.user_service_topic, self.payment_service_topic]


def _parse_bool(value: str) -> bool:
    """Parse a string into a boolean. Accepts 'true', '1', 'yes' (case-insensitive)."""
    return value.strip().lower() in ("true", "1", "yes")


def load_config() -> Settings:
    """Load settings from environment variables.

    Each Settings field can be overridden by an uppercase environment variable
    of the same name (e.g., BOOTSTRAP_SERVERS, USE_INTERNAL_LISTENERS).

    Boolean fields accept 'true', '1', or 'yes' (case-insensitive) as truthy values;
    everything else is treated as False.
    """
    kwargs: dict = {}
    defaults = Settings()

    for f in fields(Settings):
        env_key = f.name.upper()
        env_val = os.environ.get(env_key)
        if env_val is not None:
            default_val = getattr(defaults, f.name)
            target_type = type(default_val)

            if target_type is bool:
                kwargs[f.name] = _parse_bool(env_val)
            else:
                try:
                    kwargs[f.name] = target_type(env_val)
                except (ValueError, TypeError):
                    # Fall back to default if conversion fails
                    pass

    return Settings(**kwargs)
