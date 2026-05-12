"""Unit tests for the C3 config loader (``src/config/settings.py``).

Coverage:

* YAML happy-path load returns a populated :class:`Settings`.
* Defaults are used when the YAML file is empty.
* Missing files raise :class:`FileNotFoundError`.
* Unknown keys are rejected (``extra='forbid'``).
* ``CHAOS_*`` environment variables override YAML values.
* ``CHAOS_CONFIG_PATH`` resolves correctly when no path argument is given.
* Field validators reject empty / duplicate / blank ``target_allowlist`` entries.
* ``log_level`` is normalized to upper-case and restricted to the standard set.
* Numeric bounds (``ge``/``gt``/``le``) are enforced.
* :func:`get_settings` returns the same instance until ``cache_clear()`` is called.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.config.settings import Settings, get_settings


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    """Ensure each test starts with a fresh :func:`get_settings` cache."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _scrub_chaos_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any ``CHAOS_*`` env vars leaking in from the host/Docker so each
    test sees a clean slate.
    """
    import os

    for key in list(os.environ.keys()):
        if key.startswith("CHAOS_"):
            monkeypatch.delenv(key, raising=False)


# --------------------------------------------------------------------------- #
# Happy path — production YAML
# --------------------------------------------------------------------------- #


def test_from_yaml_loads_production_config() -> None:
    """Loading the shipped YAML produces matching fields."""
    settings = Settings.from_yaml("/app/config/safety_config.yaml")

    assert isinstance(settings, Settings)
    assert settings.max_concurrent_scenarios == 3
    assert settings.cpu_emergency_threshold_pct == 90.0
    assert settings.mem_emergency_threshold_pct == 90.0
    assert settings.target_allowlist == ["log-producer", "log-consumer", "redis"]
    assert settings.metrics_collection_interval_seconds == 5.0
    assert settings.metrics_history_size == 1000
    assert settings.recovery_test_timeout_seconds == 30.0
    assert settings.recovery_grace_period_seconds == 5.0
    assert settings.default_experiment_duration == 300
    assert settings.default_severity == 2
    assert settings.docker_socket_path == "/var/run/docker.sock"
    assert settings.chaos_network_name == "chaos-net"


# --------------------------------------------------------------------------- #
# Defaults — empty YAML
# --------------------------------------------------------------------------- #


def test_from_yaml_empty_file_uses_defaults(tmp_path: Path) -> None:
    """An empty YAML file should yield a :class:`Settings` built from defaults."""
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")

    settings = Settings.from_yaml(empty)

    assert settings.max_concurrent_scenarios == 3
    assert settings.metrics_history_size == 1000
    assert settings.cpu_emergency_threshold_pct == 90.0
    assert settings.target_allowlist == ["log-producer", "log-consumer", "redis"]
    assert settings.log_level == "INFO"


# --------------------------------------------------------------------------- #
# Missing file
# --------------------------------------------------------------------------- #


def test_from_yaml_missing_file_raises(tmp_path: Path) -> None:
    """A non-existent path should raise :class:`FileNotFoundError`."""
    with pytest.raises(FileNotFoundError):
        Settings.from_yaml(tmp_path / "nope.yaml")


# --------------------------------------------------------------------------- #
# Unknown YAML key (extra=forbid)
# --------------------------------------------------------------------------- #


def test_from_yaml_unknown_field_rejected(tmp_path: Path) -> None:
    """Unknown keys must trip ``extra='forbid'`` and raise ValidationError."""
    bad = tmp_path / "extra.yaml"
    bad.write_text("unknown_field: 42\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        Settings.from_yaml(bad)


# --------------------------------------------------------------------------- #
# Env var overrides
# --------------------------------------------------------------------------- #


def test_env_overrides_yaml_int_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An int env var should win over the YAML value."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("max_concurrent_scenarios: 3\n", encoding="utf-8")

    monkeypatch.setenv("CHAOS_MAX_CONCURRENT_SCENARIOS", "7")

    settings = Settings.from_yaml(yaml_path)

    assert settings.max_concurrent_scenarios == 7


def test_env_overrides_yaml_float_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A float env var should be coerced and win over the YAML value."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("cpu_emergency_threshold_pct: 90.0\n", encoding="utf-8")

    monkeypatch.setenv("CHAOS_CPU_EMERGENCY_THRESHOLD_PCT", "75.5")

    settings = Settings.from_yaml(yaml_path)

    assert settings.cpu_emergency_threshold_pct == 75.5


def test_chaos_config_path_env_resolves_when_no_arg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``CHAOS_CONFIG_PATH`` should pick the YAML file when no path arg is given."""
    custom = tmp_path / "custom.yaml"
    custom.write_text("max_concurrent_scenarios: 9\n", encoding="utf-8")

    monkeypatch.setenv("CHAOS_CONFIG_PATH", str(custom))

    settings = Settings.from_yaml()

    assert settings.max_concurrent_scenarios == 9


# --------------------------------------------------------------------------- #
# target_allowlist field validator
# --------------------------------------------------------------------------- #


def test_target_allowlist_empty_rejected(tmp_path: Path) -> None:
    """An empty ``target_allowlist`` list should fail validation."""
    bad = tmp_path / "config.yaml"
    bad.write_text("target_allowlist: []\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        Settings.from_yaml(bad)


def test_target_allowlist_duplicates_rejected(tmp_path: Path) -> None:
    """Duplicate entries in ``target_allowlist`` should fail validation."""
    bad = tmp_path / "config.yaml"
    bad.write_text(
        textwrap.dedent(
            """\
            target_allowlist:
              - a
              - a
              - b
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        Settings.from_yaml(bad)


def test_target_allowlist_empty_string_rejected(tmp_path: Path) -> None:
    """Blank/whitespace-only entries in ``target_allowlist`` should fail."""
    bad = tmp_path / "config.yaml"
    # Quoted empty string so YAML produces "" not None.
    bad.write_text(
        textwrap.dedent(
            """\
            target_allowlist:
              - ""
              - "ok"
            """
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        Settings.from_yaml(bad)


# --------------------------------------------------------------------------- #
# log_level field validator
# --------------------------------------------------------------------------- #


def test_log_level_normalized_to_upper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``log_level`` should be normalized to upper-case."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_LOG_LEVEL", "debug")

    settings = Settings.from_yaml(yaml_path)

    assert settings.log_level == "DEBUG"


def test_log_level_unknown_value_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``log_level`` values outside the standard set must fail."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_LOG_LEVEL", "VERBOSE")

    with pytest.raises(ValidationError):
        Settings.from_yaml(yaml_path)


# --------------------------------------------------------------------------- #
# Numeric bounds
# --------------------------------------------------------------------------- #


def test_max_concurrent_scenarios_zero_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``max_concurrent_scenarios`` has ``ge=1``."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_MAX_CONCURRENT_SCENARIOS", "0")

    with pytest.raises(ValidationError):
        Settings.from_yaml(yaml_path)


def test_cpu_emergency_threshold_zero_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``cpu_emergency_threshold_pct`` has ``gt=0`` (strict)."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_CPU_EMERGENCY_THRESHOLD_PCT", "0")

    with pytest.raises(ValidationError):
        Settings.from_yaml(yaml_path)


def test_cpu_emergency_threshold_above_100_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``cpu_emergency_threshold_pct`` has ``le=100``."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_CPU_EMERGENCY_THRESHOLD_PCT", "101")

    with pytest.raises(ValidationError):
        Settings.from_yaml(yaml_path)


def test_default_severity_above_5_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``default_severity`` has ``le=5``."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("CHAOS_DEFAULT_SEVERITY", "6")

    with pytest.raises(ValidationError):
        Settings.from_yaml(yaml_path)


# --------------------------------------------------------------------------- #
# get_settings cache behavior
# --------------------------------------------------------------------------- #


def test_get_settings_returns_same_instance() -> None:
    """``get_settings`` is cached via :func:`functools.lru_cache`."""
    get_settings.cache_clear()

    first = get_settings()
    second = get_settings()

    assert first is second


def test_get_settings_cache_holds_until_cleared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutating env after the first call should NOT change the cached object."""
    get_settings.cache_clear()
    first = get_settings()

    # Mutate the env after the cache is warm.
    monkeypatch.setenv("CHAOS_MAX_CONCURRENT_SCENARIOS", "11")

    # Still the same cached object — env change does not invalidate.
    second = get_settings()
    assert second is first
    assert second.max_concurrent_scenarios == first.max_concurrent_scenarios

    # After cache_clear, a fresh load picks up the env override.
    get_settings.cache_clear()
    third = get_settings()
    assert third is not first
    assert third.max_concurrent_scenarios == 11
