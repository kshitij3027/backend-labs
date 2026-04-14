"""Environment-variable based configuration for the real-time analytics dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded from environment variables."""

    redis_host: str = "localhost"
    redis_port: int = 6379
    server_host: str = "0.0.0.0"
    server_port: int = 8000
    anomaly_zscore_threshold: float = 2.5
    trend_window_minutes: float = 5.0
    metric_ttl_seconds: int = 3600
    ws_heartbeat_interval: float = 30.0
    ws_broadcast_interval: float = 5.0


def get_config() -> Config:
    """Build a `Config` instance from the current process environment."""
    return Config(
        redis_host=_env_str("REDIS_HOST", "localhost"),
        redis_port=_env_int("REDIS_PORT", 6379),
        server_host=_env_str("SERVER_HOST", "0.0.0.0"),
        server_port=_env_int("SERVER_PORT", 8000),
        anomaly_zscore_threshold=_env_float("ANOMALY_ZSCORE_THRESHOLD", 2.5),
        trend_window_minutes=_env_float("TREND_WINDOW_MINUTES", 5.0),
        metric_ttl_seconds=_env_int("METRIC_TTL_SECONDS", 3600),
        ws_heartbeat_interval=_env_float("WS_HEARTBEAT_INTERVAL", 30.0),
        ws_broadcast_interval=_env_float("WS_BROADCAST_INTERVAL", 5.0),
    )
