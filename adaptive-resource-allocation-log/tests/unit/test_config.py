"""Unit tests for :mod:`src.config` covering precedence: defaults -> YAML -> env."""

import textwrap

import pytest

from src.config import Settings, load_config, get_config


# The full default-config YAML, mirroring config/config.yaml. Written to a temp file
# in tests so the suite stays hermetic (no dependency on the repo file's location).
DEFAULT_YAML = textwrap.dedent(
    """\
    dashboard:
      host: "0.0.0.0"
      port: 8080
    scaling:
      cpu_threshold_scale_up: 75
      cpu_threshold_scale_down: 40
      memory_threshold_scale_up: 80
      memory_threshold_scale_down: 50
      util_threshold_scale_up: 75
      util_threshold_scale_down: 40
      min_workers: 2
      max_workers: 20
      cooldown_period_seconds: 60
      scale_down_cooldown_seconds: 120
    monitoring:
      interval_seconds: 5
      history_window_minutes: 15
      metrics_retention_hours: 24
    forecast:
      alpha: 0.25
      beta: 0.10
      horizon_minutes: 10
      confidence_threshold: 0.70
    workload:
      base_arrival_rate: 500
      capacity_per_worker: 400
    """
)


@pytest.fixture(autouse=True)
def _clean_config_env(monkeypatch):
    """Ensure no stray env vars (incl. CONFIG_PATH) leak between tests."""
    for field_name in vars(Settings()).keys():
        monkeypatch.delenv(field_name.upper(), raising=False)
    monkeypatch.delenv("CONFIG_PATH", raising=False)
    monkeypatch.delenv("USE_DOCKER", raising=False)


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
def test_settings_defaults():
    """The dataclass defaults match the documented spec values and types."""
    s = Settings()

    assert s.host == "0.0.0.0"
    assert s.port == 8080
    assert s.log_level == "INFO"

    assert s.cpu_threshold_scale_up == 75.0
    assert s.cpu_threshold_scale_down == 40.0
    assert s.memory_threshold_scale_up == 80.0
    assert s.memory_threshold_scale_down == 50.0
    assert s.util_threshold_scale_up == 75.0
    assert s.util_threshold_scale_down == 40.0

    assert s.min_workers == 2
    assert s.max_workers == 20

    assert s.cooldown_period_seconds == 60.0
    assert s.scale_down_cooldown_seconds == 120.0

    assert s.monitoring_interval_seconds == 5.0
    assert s.orchestration_interval_seconds == 5.0

    assert s.history_window_minutes == 15
    assert s.metrics_retention_hours == 24

    assert s.forecast_alpha == 0.25
    assert s.forecast_beta == 0.10
    assert s.horizon_minutes == 10
    assert s.confidence_threshold == 0.70

    assert s.base_arrival_rate == 500.0
    assert s.capacity_per_worker == 400.0

    assert s.worker_backend == "simulated"
    assert s.worker_image == "adaptive-worker:latest"
    assert s.ws_emit_interval == 2.0


def test_load_config_no_yaml_no_env_returns_defaults(monkeypatch, tmp_path):
    """With no YAML present and no env, load_config equals the bare defaults."""
    missing = tmp_path / "does_not_exist.yaml"
    assert load_config(str(missing)) == Settings()


def test_get_config_is_alias(monkeypatch, tmp_path):
    """get_config delegates to load_config with the same semantics."""
    missing = tmp_path / "nope.yaml"
    assert get_config(str(missing)) == Settings()


# --------------------------------------------------------------------------- #
# YAML loading
# --------------------------------------------------------------------------- #
def test_yaml_loads_documented_values(write_yaml):
    """Loading the default-config YAML yields the documented field values."""
    path = write_yaml(DEFAULT_YAML)
    s = load_config(path)

    # dashboard section -> flat fields
    assert s.host == "0.0.0.0"
    assert s.port == 8080
    # scaling section
    assert s.cpu_threshold_scale_up == 75.0
    assert s.cpu_threshold_scale_down == 40.0
    assert s.memory_threshold_scale_up == 80.0
    assert s.util_threshold_scale_up == 75.0
    assert s.min_workers == 2
    assert s.max_workers == 20
    assert s.cooldown_period_seconds == 60.0
    assert s.scale_down_cooldown_seconds == 120.0
    # monitoring section (note: interval_seconds -> monitoring_interval_seconds)
    assert s.monitoring_interval_seconds == 5.0
    assert s.history_window_minutes == 15
    assert s.metrics_retention_hours == 24
    # forecast section (alpha -> forecast_alpha, beta -> forecast_beta)
    assert s.forecast_alpha == 0.25
    assert s.forecast_beta == 0.10
    assert s.horizon_minutes == 10
    assert s.confidence_threshold == 0.70
    # workload section
    assert s.base_arrival_rate == 500.0
    assert s.capacity_per_worker == 400.0


def test_yaml_types_are_coerced(write_yaml):
    """Integer-looking YAML threshold values land as floats on float fields."""
    path = write_yaml(DEFAULT_YAML)
    s = load_config(path)

    # YAML wrote `75` (int) but the field is a float — confirm coercion.
    assert isinstance(s.cpu_threshold_scale_up, float)
    assert isinstance(s.monitoring_interval_seconds, float)
    # And an int field stays an int.
    assert isinstance(s.min_workers, int)
    assert isinstance(s.port, int)


def test_yaml_accepts_flat_keys(write_yaml):
    """Flat top-level field names (not just nested sections) are honored."""
    path = write_yaml("min_workers: 7\nport: 7777\n")
    s = load_config(path)
    assert s.min_workers == 7
    assert s.port == 7777


def test_yaml_partial_override_keeps_other_defaults(write_yaml):
    """A YAML overriding one section leaves untouched fields at their defaults."""
    path = write_yaml("scaling:\n  min_workers: 3\n")
    s = load_config(path)
    assert s.min_workers == 3
    assert s.max_workers == 20          # untouched default
    assert s.port == 8080               # untouched default


def test_yaml_unknown_keys_ignored(write_yaml):
    """Unknown top-level keys and unknown nested keys are ignored gracefully."""
    path = write_yaml(
        "totally_unknown: 1\n"
        "scaling:\n"
        "  min_workers: 9\n"
        "  bogus_key: 123\n"
    )
    s = load_config(path)
    assert s.min_workers == 9
    assert not hasattr(s, "totally_unknown")
    assert not hasattr(s, "bogus_key")


# --------------------------------------------------------------------------- #
# Environment overrides (env beats YAML and defaults)
# --------------------------------------------------------------------------- #
def test_env_overrides_yaml_and_defaults(monkeypatch, write_yaml):
    """Env vars win over both YAML and defaults, with correct type coercion."""
    path = write_yaml(DEFAULT_YAML)  # YAML sets min_workers=2, port=8080

    monkeypatch.setenv("MIN_WORKERS", "5")
    monkeypatch.setenv("PORT", "9090")
    monkeypatch.setenv("CPU_THRESHOLD_SCALE_UP", "88.5")

    s = load_config(path)

    assert s.min_workers == 5
    assert isinstance(s.min_workers, int)
    assert s.port == 9090
    assert isinstance(s.port, int)
    assert s.cpu_threshold_scale_up == 88.5
    assert isinstance(s.cpu_threshold_scale_up, float)


def test_env_overrides_without_yaml(monkeypatch, tmp_path):
    """Env overrides apply even when there's no YAML file at all."""
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("MAX_WORKERS", "42")
    monkeypatch.setenv("HOST", "127.0.0.1")
    monkeypatch.setenv("FORECAST_ALPHA", "0.5")

    s = load_config(str(missing))

    assert s.max_workers == 42
    assert s.host == "127.0.0.1"
    assert s.forecast_alpha == 0.5


def test_env_config_path_is_respected(monkeypatch, write_yaml):
    """CONFIG_PATH env var selects the YAML file when no arg is given."""
    path = write_yaml("scaling:\n  max_workers: 33\n")
    monkeypatch.setenv("CONFIG_PATH", path)

    s = load_config()  # no explicit path -> read CONFIG_PATH
    assert s.max_workers == 33


def test_malformed_env_value_falls_back(monkeypatch, tmp_path):
    """A non-numeric env value for an int field is ignored, keeping the default."""
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("MIN_WORKERS", "not-a-number")
    s = load_config(str(missing))
    assert s.min_workers == 2  # default retained, no crash


# --------------------------------------------------------------------------- #
# USE_DOCKER toggle
# --------------------------------------------------------------------------- #
def test_use_docker_selects_docker_backend(monkeypatch, tmp_path):
    """USE_DOCKER=1 flips worker_backend to 'docker'."""
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("USE_DOCKER", "1")
    s = load_config(str(missing))
    assert s.worker_backend == "docker"


def test_default_backend_is_simulated(tmp_path):
    """Without USE_DOCKER, the backend stays 'simulated'."""
    missing = tmp_path / "missing.yaml"
    s = load_config(str(missing))
    assert s.worker_backend == "simulated"


def test_worker_backend_env_explicit(monkeypatch, tmp_path):
    """WORKER_BACKEND env var can set the backend directly."""
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("WORKER_BACKEND", "docker")
    s = load_config(str(missing))
    assert s.worker_backend == "docker"


# --------------------------------------------------------------------------- #
# Defensive YAML handling
# --------------------------------------------------------------------------- #
def test_missing_yaml_does_not_raise(tmp_path):
    """A non-existent YAML path returns defaults without raising."""
    missing = tmp_path / "absent.yaml"
    s = load_config(str(missing))
    assert s == Settings()


def test_malformed_yaml_does_not_raise(write_yaml):
    """Invalid YAML content falls back to defaults+env rather than crashing."""
    # Unbalanced brackets / invalid mapping -> yaml.YAMLError, swallowed by loader.
    path = write_yaml("scaling: [this is : not valid: yaml\n  min_workers: 2\n")
    s = load_config(path)
    assert s == Settings()


def test_empty_yaml_returns_defaults(write_yaml):
    """An empty YAML document (parses to None) yields defaults."""
    path = write_yaml("")
    s = load_config(path)
    assert s == Settings()


def test_non_mapping_yaml_returns_defaults(write_yaml):
    """A YAML document that is a list/scalar (not a mapping) yields defaults."""
    path = write_yaml("- just\n- a\n- list\n")
    s = load_config(path)
    assert s == Settings()
