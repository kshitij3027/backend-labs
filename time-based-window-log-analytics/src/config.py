"""Application configuration with YAML and environment variable support."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class WindowTypeConfig:
    """Configuration for a single window type."""

    name: str
    size_seconds: int
    grace_period_seconds: int
    retention_seconds: int


@dataclass
class AppConfig:
    """Top-level application configuration."""

    redis_host: str = "localhost"
    redis_port: int = 6379
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    log_level: str = "INFO"
    window_types: list[WindowTypeConfig] = field(default_factory=list)
    dashboard_refresh_interval: int = 5
    lifecycle_check_interval: int = 10
    cleanup_interval: int = 60

    @classmethod
    def from_env(cls) -> AppConfig:
        """Build config from environment variables, falling back to config.yaml."""
        # Try loading from YAML first for defaults
        yaml_path = Path("config.yaml")
        if yaml_path.exists():
            config = cls.load_yaml(str(yaml_path))
        else:
            config = cls()

        # Override with env vars where present
        config.redis_host = os.environ.get("REDIS_HOST", config.redis_host)
        config.redis_port = int(os.environ.get("REDIS_PORT", config.redis_port))
        config.api_host = os.environ.get("API_HOST", config.api_host)
        config.api_port = int(os.environ.get("API_PORT", config.api_port))
        config.log_level = os.environ.get("LOG_LEVEL", config.log_level)

        return config

    @staticmethod
    def load_yaml(path: str) -> AppConfig:
        """Load configuration from a YAML file."""
        with open(path, "r") as f:
            raw = yaml.safe_load(f)

        window_types = [
            WindowTypeConfig(
                name=wt["name"],
                size_seconds=wt["size_seconds"],
                grace_period_seconds=wt["grace_period_seconds"],
                retention_seconds=wt["retention_seconds"],
            )
            for wt in raw.get("window_types", [])
        ]

        redis_cfg = raw.get("redis", {})
        api_cfg = raw.get("api", {})
        dashboard_cfg = raw.get("dashboard", {})
        lifecycle_cfg = raw.get("lifecycle", {})

        return AppConfig(
            redis_host=redis_cfg.get("host", "localhost"),
            redis_port=redis_cfg.get("port", 6379),
            api_host=api_cfg.get("host", "0.0.0.0"),
            api_port=api_cfg.get("port", 8080),
            window_types=window_types,
            dashboard_refresh_interval=dashboard_cfg.get("refresh_interval", 5),
            lifecycle_check_interval=lifecycle_cfg.get("check_interval", 10),
            cleanup_interval=lifecycle_cfg.get("cleanup_interval", 60),
        )
