"""Application configuration with environment variable overrides."""

import os
from dataclasses import dataclass, fields


@dataclass
class Settings:
    """Central configuration for the Kafka log compaction state manager."""

    # Kafka connection
    bootstrap_servers: str = "localhost:9092"
    bootstrap_servers_internal: str = "kafka:29092"
    use_internal_listeners: bool = False  # Set to True inside Docker

    # Topic
    topic_name: str = "user-profiles"

    # Producer
    num_users: int = 10
    update_interval_seconds: float = 1.0

    # Consumer
    consumer_group_id: str = "state-consumer-group"

    # Dashboard
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 5555

    # Compaction tuning
    segment_bytes: int = 1048576  # 1MB for fast compaction
    min_cleanable_dirty_ratio: float = 0.1
    delete_retention_ms: int = 60000  # 60s for demo
    max_compaction_lag_ms: int = 60000

    @property
    def active_bootstrap_servers(self) -> str:
        """Return internal or external bootstrap servers based on listener mode."""
        return self.bootstrap_servers_internal if self.use_internal_listeners else self.bootstrap_servers


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
