"""Configuration loader — YAML base with environment variable overrides."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import yaml


class Config:
    """Application configuration loaded from YAML with env var overrides."""

    # Mapping of env var names to config attribute names
    ENV_MAP = {
        "REDIS_URL": "redis_url",
        "STREAM_KEY": "stream_key",
        "CONSUMER_GROUP": "consumer_group",
        "CONSUMER_NAME": "consumer_name",
        "NUM_WORKERS": "num_workers",
        "BATCH_SIZE": "batch_size",
        "BLOCK_MS": "block_ms",
        "DASHBOARD_PORT": "dashboard_port",
        "METRICS_WINDOW_SEC": "metrics_window_sec",
        "MAX_RETRIES": "max_retries",
        "RETRY_BASE_DELAY": "retry_base_delay",
        "RETRY_MAX_DELAY": "retry_max_delay",
        "DLQ_STREAM_KEY": "dlq_stream_key",
        "IDEMPOTENCY_TTL": "idempotency_ttl",
    }

    INT_FIELDS = {
        "num_workers", "batch_size", "block_ms", "dashboard_port",
        "metrics_window_sec", "max_retries", "idempotency_ttl",
    }
    FLOAT_FIELDS = {"retry_base_delay", "retry_max_delay"}

    def __init__(self, **kwargs):
        self.redis_url: str = kwargs.get("redis_url", "redis://localhost:6379")
        self.stream_key: str = kwargs.get("stream_key", "logs:access")
        self.consumer_group: str = kwargs.get("consumer_group", "log-processors")
        self.consumer_name: str = kwargs.get("consumer_name", f"consumer-{socket.gethostname()}")
        self.num_workers: int = kwargs.get("num_workers", 4)
        self.batch_size: int = kwargs.get("batch_size", 100)
        self.block_ms: int = kwargs.get("block_ms", 2000)
        self.dashboard_port: int = kwargs.get("dashboard_port", 8000)
        self.metrics_window_sec: int = kwargs.get("metrics_window_sec", 300)
        self.max_retries: int = kwargs.get("max_retries", 3)
        self.retry_base_delay: float = kwargs.get("retry_base_delay", 1.0)
        self.retry_max_delay: float = kwargs.get("retry_max_delay", 30.0)
        self.dlq_stream_key: str = kwargs.get("dlq_stream_key", "logs:dlq")
        self.idempotency_ttl: int = kwargs.get("idempotency_ttl", 3600)

    @classmethod
    def load(cls, config_path: str | None = None) -> Config:
        """Load config from YAML file, then apply env var overrides."""
        data = {}
        if config_path is None:
            config_path = os.environ.get("CONFIG_PATH", "config.yaml")
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}

        # Apply env var overrides
        for env_var, attr_name in cls.ENV_MAP.items():
            value = os.environ.get(env_var)
            if value is not None:
                if attr_name in cls.INT_FIELDS:
                    data[attr_name] = int(value)
                elif attr_name in cls.FLOAT_FIELDS:
                    data[attr_name] = float(value)
                else:
                    data[attr_name] = value

        return cls(**data)
