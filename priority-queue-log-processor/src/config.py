"""Application configuration with environment variable overrides."""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Central configuration for the priority queue log processor."""

    max_queue_size: int = 10000
    num_workers: int = 4
    dashboard_port: int = 8080
    dashboard_refresh_interval: float = 2.0

    # Per-priority processing time targets (ms)
    critical_process_time_ms: int = 10
    high_process_time_ms: int = 50
    medium_process_time_ms: int = 100
    low_process_time_ms: int = 200

    # Priority aging
    aging_threshold_seconds: float = 300.0
    aging_check_interval: float = 10.0

    # Dynamic scaling
    scale_up_threshold: float = 0.8
    scale_down_threshold: float = 0.2
    min_workers: int = 2
    max_workers: int = 16

    # Backpressure watermarks (fraction of max_queue_size)
    backpressure_low_watermark: float = 0.8
    backpressure_medium_watermark: float = 0.9
    backpressure_high_watermark: float = 0.95

    # Load generator
    generator_rate: float = 100.0

    # Alerting
    alert_queue_depth_threshold: int = 8000


def load_config() -> Settings:
    """Load settings from environment variables.

    Each Settings field can be overridden by an uppercase environment variable
    of the same name (e.g., MAX_QUEUE_SIZE, NUM_WORKERS).
    """
    kwargs: dict = {}
    defaults = Settings()

    for field_name in Settings.__dataclass_fields__:
        env_key = field_name.upper()
        env_val = os.environ.get(env_key)
        if env_val is not None:
            default_val = getattr(defaults, field_name)
            target_type = type(default_val)
            try:
                kwargs[field_name] = target_type(env_val)
            except (ValueError, TypeError):
                # Fall back to default if conversion fails
                pass

    return Settings(**kwargs)
