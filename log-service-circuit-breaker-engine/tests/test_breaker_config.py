"""Tests for CircuitBreakerConfig defaults, env loading, and exception/state types."""
from __future__ import annotations

import dataclasses

import pytest

from src.config import (
    CircuitBreakerConfig,
    critical_service_config,
    load_config_from_env,
    standard_service_config,
)
from src.exceptions import (
    CircuitBreakerError,
    CircuitBreakerOpenException,
    CircuitBreakerTimeoutException,
)
from src.state import CircuitState, StateChange


# ---------------------------------------------------------------------------
# CircuitBreakerConfig
# ---------------------------------------------------------------------------


def test_default_config() -> None:
    """All tunable fields use the spec'd defaults when only ``name`` is set."""
    cfg = CircuitBreakerConfig(name="x")
    assert cfg.name == "x"
    assert cfg.failure_threshold == 5
    assert cfg.recovery_timeout == 60.0
    assert cfg.expected_exception is Exception
    assert cfg.timeout_duration == 10.0
    assert cfg.half_open_max_calls == 3
    assert cfg.monitoring_window == 60.0
    assert cfg.error_rate_threshold == 0.5
    assert cfg.slow_call_duration_threshold == 2.0
    assert cfg.consecutive_failures_threshold == 5
    assert cfg.min_volume_threshold == 10


def test_override_via_constructor() -> None:
    """Explicit constructor args override every default."""
    cfg = CircuitBreakerConfig(
        name="svc",
        failure_threshold=42,
        recovery_timeout=1.5,
        timeout_duration=0.25,
        half_open_max_calls=7,
        monitoring_window=120.0,
        error_rate_threshold=0.9,
        slow_call_duration_threshold=4.0,
        consecutive_failures_threshold=11,
        min_volume_threshold=2,
    )
    assert cfg.failure_threshold == 42
    assert cfg.recovery_timeout == 1.5
    assert cfg.timeout_duration == 0.25
    assert cfg.half_open_max_calls == 7
    assert cfg.monitoring_window == 120.0
    assert cfg.error_rate_threshold == 0.9
    assert cfg.slow_call_duration_threshold == 4.0
    assert cfg.consecutive_failures_threshold == 11
    assert cfg.min_volume_threshold == 2


_ENV_KEYS = (
    "CB_DEFAULT_FAILURE_THRESHOLD",
    "CB_DEFAULT_RECOVERY_TIMEOUT",
    "CB_DEFAULT_TIMEOUT_DURATION",
    "CB_DEFAULT_HALF_OPEN_MAX_CALLS",
    "CB_DEFAULT_MONITORING_WINDOW",
    "CB_DEFAULT_ERROR_RATE_THRESHOLD",
    "CB_DEFAULT_SLOW_CALL_DURATION_THRESHOLD",
    "CB_DEFAULT_CONSECUTIVE_FAILURES_THRESHOLD",
    "CB_DEFAULT_MIN_VOLUME_THRESHOLD",
)


def test_load_config_from_env_uses_defaults_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no env vars set, the loaded config matches the model defaults."""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    loaded = load_config_from_env("svc")
    expected = CircuitBreakerConfig(name="svc")

    assert loaded.failure_threshold == expected.failure_threshold
    assert loaded.recovery_timeout == expected.recovery_timeout
    assert loaded.timeout_duration == expected.timeout_duration
    assert loaded.half_open_max_calls == expected.half_open_max_calls
    assert loaded.monitoring_window == expected.monitoring_window
    assert loaded.error_rate_threshold == expected.error_rate_threshold
    assert loaded.slow_call_duration_threshold == expected.slow_call_duration_threshold
    assert (
        loaded.consecutive_failures_threshold
        == expected.consecutive_failures_threshold
    )
    assert loaded.min_volume_threshold == expected.min_volume_threshold


def test_load_config_from_env_reads_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Env-var overrides flow through with correct int/float coercion."""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CB_DEFAULT_FAILURE_THRESHOLD", "9")
    monkeypatch.setenv("CB_DEFAULT_RECOVERY_TIMEOUT", "12.5")

    loaded = load_config_from_env("svc")

    assert loaded.failure_threshold == 9
    assert isinstance(loaded.failure_threshold, int)
    assert loaded.recovery_timeout == 12.5
    assert isinstance(loaded.recovery_timeout, float)
    # Untouched fields still use defaults.
    assert loaded.timeout_duration == 10.0


def test_load_config_from_env_respects_custom_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-default ``prefix`` redirects the env-var lookup."""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CB_DB_FAILURE_THRESHOLD", "2")

    loaded = load_config_from_env("db", prefix="CB_DB_")
    assert loaded.failure_threshold == 2


def test_critical_preset_lower_thresholds() -> None:
    """The critical preset trips faster and recovers more aggressively."""
    cfg = critical_service_config("db")
    assert cfg.name == "db"
    assert cfg.failure_threshold == 3
    assert cfg.recovery_timeout == 30.0
    assert cfg.timeout_duration == 5.0
    assert cfg.half_open_max_calls == 2
    assert cfg.consecutive_failures_threshold == 3
    assert cfg.error_rate_threshold == 0.3


def test_standard_preset_matches_defaults() -> None:
    """The standard preset is just ``CircuitBreakerConfig(name=name)``."""
    standard = standard_service_config("api")
    expected = CircuitBreakerConfig(name="api")
    assert standard.model_dump() == expected.model_dump()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


def test_open_exception_carries_attributes() -> None:
    exc = CircuitBreakerOpenException("svc", 1234.5)
    assert exc.breaker_name == "svc"
    assert exc.opened_at == 1234.5
    assert "OPEN" in str(exc)
    assert isinstance(exc, CircuitBreakerError)


def test_timeout_exception_carries_attributes() -> None:
    exc = CircuitBreakerTimeoutException("svc", 7.5)
    assert exc.breaker_name == "svc"
    assert exc.timeout_seconds == 7.5
    assert "7.5" in str(exc)
    assert "timed out" in str(exc)
    assert isinstance(exc, CircuitBreakerError)


# ---------------------------------------------------------------------------
# CircuitState + StateChange
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("member", "expected"),
    [
        (CircuitState.CLOSED, "CLOSED"),
        (CircuitState.OPEN, "OPEN"),
        (CircuitState.HALF_OPEN, "HALF_OPEN"),
    ],
)
def test_state_enum_values(member: CircuitState, expected: str) -> None:
    """Every enum member's ``.value`` matches its name as a string."""
    assert member.value == expected
    assert isinstance(member, str)


def test_state_change_dataclass_immutable() -> None:
    """StateChange is a frozen dataclass that round-trips its fields."""
    change = StateChange(
        breaker_name="svc",
        from_state=CircuitState.CLOSED,
        to_state=CircuitState.OPEN,
        timestamp=42.0,
        reason="threshold exceeded",
    )
    assert change.breaker_name == "svc"
    assert change.from_state is CircuitState.CLOSED
    assert change.to_state is CircuitState.OPEN
    assert change.timestamp == 42.0
    assert change.reason == "threshold exceeded"

    with pytest.raises(dataclasses.FrozenInstanceError):
        change.reason = "mutated"  # type: ignore[misc]
