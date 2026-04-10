"""Environment-variable based configuration for the sliding-window analytics engine."""

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

    window_size_seconds: float = 30.0
    slide_interval_seconds: float = 5.0
    max_event_buffer_size: int = 10000
    redis_host: str = "localhost"
    redis_port: int = 6379
    api_port: int = 8000
    ws_update_interval_seconds: float = 5.0
    spike_probability: float = 0.1
    # When true, the FastAPI lifespan will NOT spawn the background
    # LogEventGenerator task. This is used by the unit-test suite so
    # tests don't race against a 600 evt/s producer.
    disable_generator: bool = False


def get_config() -> Config:
    """Build a `Config` instance from the current process environment."""
    return Config(
        window_size_seconds=_env_float("WINDOW_SIZE_SECONDS", 30.0),
        slide_interval_seconds=_env_float("SLIDE_INTERVAL_SECONDS", 5.0),
        max_event_buffer_size=_env_int("MAX_EVENT_BUFFER_SIZE", 10000),
        redis_host=_env_str("REDIS_HOST", "localhost"),
        redis_port=_env_int("REDIS_PORT", 6379),
        api_port=_env_int("API_PORT", 8000),
        ws_update_interval_seconds=_env_float("WS_UPDATE_INTERVAL_SECONDS", 5.0),
        spike_probability=_env_float("SPIKE_PROBABILITY", 0.1),
        disable_generator=_env_bool("DISABLE_GENERATOR", False),
    )
