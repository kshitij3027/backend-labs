"""Environment-variable based configuration for the sessionization engine."""
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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded from environment variables."""
    redis_url: str = "redis://redis:6379/0"
    session_timeout_seconds: float = 1800.0  # 30 minutes
    session_max_duration_seconds: float = 14400.0  # 4 hours
    host: str = "0.0.0.0"
    port: int = 8000
    num_partitions: int = 4
    ws_push_interval_seconds: float = 2.0
    cleanup_interval_seconds: float = 60.0
    simulator_enabled: bool = True
    simulator_users: int = 50
    simulator_events_per_second: float = 10.0
    disable_simulator: bool = False
    device_change_boundary: bool = False
    timestamp_tolerance_seconds: float = 5.0
    dedup_window_seconds: float = 2.0
    merge_threshold: float = 0.7
    merge_window_seconds: float = 300.0


def get_config() -> Config:
    """Build a Config instance from the current process environment."""
    return Config(
        redis_url=_env_str("REDIS_URL", "redis://redis:6379/0"),
        session_timeout_seconds=_env_float("SESSION_TIMEOUT_SECONDS", 1800.0),
        session_max_duration_seconds=_env_float("SESSION_MAX_DURATION_SECONDS", 14400.0),
        host=_env_str("HOST", "0.0.0.0"),
        port=_env_int("PORT", 8000),
        num_partitions=_env_int("NUM_PARTITIONS", 4),
        ws_push_interval_seconds=_env_float("WS_PUSH_INTERVAL_SECONDS", 2.0),
        cleanup_interval_seconds=_env_float("CLEANUP_INTERVAL_SECONDS", 60.0),
        simulator_enabled=_env_bool("SIMULATOR_ENABLED", True),
        simulator_users=_env_int("SIMULATOR_USERS", 50),
        simulator_events_per_second=_env_float("SIMULATOR_EVENTS_PER_SECOND", 10.0),
        disable_simulator=_env_bool("DISABLE_SIMULATOR", False),
        device_change_boundary=_env_bool("DEVICE_CHANGE_BOUNDARY", False),
        timestamp_tolerance_seconds=_env_float("TIMESTAMP_TOLERANCE_SECONDS", 5.0),
        dedup_window_seconds=_env_float("DEDUP_WINDOW_SECONDS", 2.0),
        merge_threshold=_env_float("MERGE_THRESHOLD", 0.7),
        merge_window_seconds=_env_float("MERGE_WINDOW_SECONDS", 300.0),
    )
