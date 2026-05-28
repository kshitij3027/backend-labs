"""Unit tests for src.settings — defaults, env overrides, and caching."""

from __future__ import annotations

import pytest

from src.settings import Settings, get_settings


def test_get_settings_returns_settings_instance() -> None:
    settings = get_settings()
    assert isinstance(settings, Settings)


def test_documented_defaults() -> None:
    """Spot-check the spec §7 configurable parameters."""
    settings = Settings()

    # Batch size bounds & seed
    assert settings.min_batch_size == 50
    assert settings.max_batch_size == 5000
    assert settings.initial_batch_size == 100

    # Optimizer dynamics
    assert settings.smoothing_alpha == pytest.approx(0.2)
    assert settings.optimization_interval == pytest.approx(5.0)
    assert settings.batch_increase_factor == pytest.approx(1.1)
    assert settings.batch_decrease_factor == pytest.approx(0.9)

    # Safety constraint thresholds
    assert settings.cpu_constraint_threshold == pytest.approx(90.0)
    assert settings.memory_constraint_threshold == pytest.approx(90.0)
    assert settings.latency_constraint_threshold == pytest.approx(1000.0)


def test_additional_defaults() -> None:
    """The remaining documented defaults round out the control loop."""
    settings = Settings()

    assert settings.default_messages_per_second == pytest.approx(100.0)
    assert settings.default_burst_probability == pytest.approx(0.2)
    assert settings.weight_throughput == pytest.approx(0.7)
    assert settings.weight_latency == pytest.approx(0.3)
    assert settings.learning_samples == 5
    assert settings.stable_gradient_threshold == pytest.approx(0.01)
    assert settings.recovery_cpu_threshold == pytest.approx(70.0)
    assert settings.recovery_memory_threshold == pytest.approx(70.0)
    assert settings.recovery_latency_threshold == pytest.approx(300.0)
    assert settings.recovery_cycles == 3
    assert settings.metrics_history_size == 200
    assert settings.dashboard_points == 20
    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 8000
    assert settings.log_level in {"INFO", "WARNING"}  # compose sets WARNING


def test_env_var_override_applies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uppercase env vars override defaults on a freshly built Settings()."""
    monkeypatch.setenv("MIN_BATCH_SIZE", "128")
    monkeypatch.setenv("SMOOTHING_ALPHA", "0.5")
    monkeypatch.setenv("CPU_CONSTRAINT_THRESHOLD", "75.5")

    # Build directly (not via the lru_cache'd get_settings) so we read the
    # current environment rather than a stale cached instance.
    settings = Settings()

    assert settings.min_batch_size == 128
    assert settings.smoothing_alpha == pytest.approx(0.5)
    assert settings.cpu_constraint_threshold == pytest.approx(75.5)


def test_env_var_override_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """case_sensitive=False means lowercase env var names also bind."""
    monkeypatch.setenv("max_batch_size", "9999")
    monkeypatch.setenv("optimization_interval", "2.5")

    settings = Settings()

    assert settings.max_batch_size == 9999
    assert settings.optimization_interval == pytest.approx(2.5)


def test_get_settings_is_cached() -> None:
    """get_settings is lru_cache'd: repeated calls return the same object."""
    get_settings.cache_clear()
    first = get_settings()
    second = get_settings()
    assert first is second

    # And the cache reports a single stored entry.
    assert get_settings.cache_info().currsize == 1
