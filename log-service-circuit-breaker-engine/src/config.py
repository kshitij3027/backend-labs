"""Circuit breaker configuration model, env loader, and tiered presets."""
from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict


class CircuitBreakerConfig(BaseModel):
    """Tunable parameters that govern a single circuit breaker instance."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: type[Exception] = Exception
    timeout_duration: float = 10.0
    half_open_max_calls: int = 3
    monitoring_window: float = 60.0
    error_rate_threshold: float = 0.5
    slow_call_duration_threshold: float = 2.0
    consecutive_failures_threshold: int = 5
    min_volume_threshold: int = 10


def _read_env_int(key: str, default: int) -> int:
    """Read an integer-valued env var, falling back to ``default`` if unset."""
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return int(raw)


def _read_env_float(key: str, default: float) -> float:
    """Read a float-valued env var, falling back to ``default`` if unset."""
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return float(raw)


def load_config_from_env(name: str, prefix: str = "CB_DEFAULT_") -> CircuitBreakerConfig:
    """Build a ``CircuitBreakerConfig`` whose fields are overridden from env vars.

    Each tunable field has an env var of the form ``{prefix}{FIELD_NAME}``.
    Unset env vars fall back to the model's defaults.
    """
    defaults = CircuitBreakerConfig(name=name)
    return CircuitBreakerConfig(
        name=name,
        failure_threshold=_read_env_int(
            f"{prefix}FAILURE_THRESHOLD", defaults.failure_threshold
        ),
        recovery_timeout=_read_env_float(
            f"{prefix}RECOVERY_TIMEOUT", defaults.recovery_timeout
        ),
        timeout_duration=_read_env_float(
            f"{prefix}TIMEOUT_DURATION", defaults.timeout_duration
        ),
        half_open_max_calls=_read_env_int(
            f"{prefix}HALF_OPEN_MAX_CALLS", defaults.half_open_max_calls
        ),
        monitoring_window=_read_env_float(
            f"{prefix}MONITORING_WINDOW", defaults.monitoring_window
        ),
        error_rate_threshold=_read_env_float(
            f"{prefix}ERROR_RATE_THRESHOLD", defaults.error_rate_threshold
        ),
        slow_call_duration_threshold=_read_env_float(
            f"{prefix}SLOW_CALL_DURATION_THRESHOLD",
            defaults.slow_call_duration_threshold,
        ),
        consecutive_failures_threshold=_read_env_int(
            f"{prefix}CONSECUTIVE_FAILURES_THRESHOLD",
            defaults.consecutive_failures_threshold,
        ),
        min_volume_threshold=_read_env_int(
            f"{prefix}MIN_VOLUME_THRESHOLD", defaults.min_volume_threshold
        ),
    )


def critical_service_config(name: str) -> CircuitBreakerConfig:
    """Preset for critical services: stricter thresholds, faster recovery."""
    return CircuitBreakerConfig(
        name=name,
        failure_threshold=3,
        recovery_timeout=30.0,
        timeout_duration=5.0,
        half_open_max_calls=2,
        consecutive_failures_threshold=3,
        error_rate_threshold=0.3,
    )


def standard_service_config(name: str) -> CircuitBreakerConfig:
    """Convenience preset for non-critical services using all defaults."""
    return CircuitBreakerConfig(name=name)
