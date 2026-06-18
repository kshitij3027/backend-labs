"""Unit tests for :mod:`src.config`.

These exercise the configuration precedence chain — dataclass defaults overridden
by a flat YAML file, in turn overridden by UPPERCASE environment variables — along
with per-field type coercion and the loader's defensive handling of missing,
malformed, or unknown input.

All tests are hermetic: ``write_yaml`` writes to pytest's ``tmp_path`` (never the
real ``config/config.yaml``) and ``monkeypatch.setenv`` auto-reverts environment
changes at teardown, so nothing leaks between tests.
"""

from __future__ import annotations

import pytest

from src.config import (
    DEFAULT_CONFIG_PATH,
    Settings,
    get_config,
    load_config,
)


def test_defaults_match_spec(config: Settings) -> None:
    """A bare ``Settings()`` carries the documented defaults (project reqs §7)."""
    # Server / dashboard.
    assert config.host == "0.0.0.0"
    assert config.port == 8000
    assert config.log_level == "INFO"
    # Filesystem locations.
    assert config.model_dir == "/app/models"
    assert config.data_dir == "/app/data"
    # Sample-data generation / reproducibility.
    assert config.sample_size == 1000
    assert config.random_seed == 42
    # TF-IDF feature extraction.
    assert config.tfidf_max_features == 5000
    assert config.tfidf_ngram_max == 2
    # Ensemble member hyperparameters.
    assert config.rf_n_estimators == 100
    assert config.gb_n_estimators == 100
    assert config.ensemble_weights == [1, 2, 3]
    # Adaptive learning loop.
    assert config.accuracy_retrain_threshold == 0.90
    assert config.drift_window == 100
    # Serving performance.
    assert config.target_latency_ms == 100
    assert config.cache_size == 1024


def test_default_config_path_constant() -> None:
    """The container default path is the documented ``/app/config/config.yaml``."""
    assert DEFAULT_CONFIG_PATH == "/app/config/config.yaml"


def test_yaml_overrides_defaults(write_yaml) -> None:
    """A flat lowercase YAML file overrides the dataclass defaults it specifies."""
    path = write_yaml({"port": 9000, "sample_size": 250, "log_level": "DEBUG"})

    settings = load_config(path)

    # Overridden by YAML...
    assert settings.port == 9000
    assert settings.sample_size == 250
    assert settings.log_level == "DEBUG"
    # ...everything untouched stays at the default.
    assert settings.host == "0.0.0.0"
    assert settings.random_seed == 42


def test_env_overrides_yaml(write_yaml, monkeypatch: pytest.MonkeyPatch) -> None:
    """An UPPERCASE env var wins over the same key set in YAML (highest precedence)."""
    path = write_yaml({"port": 9000, "log_level": "DEBUG"})
    monkeypatch.setenv("PORT", "9100")

    settings = load_config(path)

    assert settings.port == 9100  # env beat YAML's 9000
    assert settings.log_level == "DEBUG"  # YAML still applies where env is silent


def test_env_type_coercion(monkeypatch: pytest.MonkeyPatch) -> None:
    """String env values are coerced to each field's declared type."""
    monkeypatch.setenv("PORT", "9001")
    monkeypatch.setenv("ACCURACY_RETRAIN_THRESHOLD", "0.8")
    monkeypatch.setenv("DRIFT_WINDOW", "55")

    settings = load_config()

    assert settings.port == 9001 and isinstance(settings.port, int)
    assert settings.accuracy_retrain_threshold == 0.8
    assert isinstance(settings.accuracy_retrain_threshold, float)
    assert settings.drift_window == 55 and isinstance(settings.drift_window, int)


def test_ensemble_weights_from_env_comma_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ENSEMBLE_WEIGHTS`` as a comma string becomes a list of numbers."""
    monkeypatch.setenv("ENSEMBLE_WEIGHTS", "5,6,7")

    settings = load_config()

    assert settings.ensemble_weights == [5, 6, 7]
    assert all(isinstance(w, int) for w in settings.ensemble_weights)


def test_ensemble_weights_from_yaml_list(write_yaml) -> None:
    """A YAML list passes straight through to ``ensemble_weights``."""
    path = write_yaml({"ensemble_weights": [2, 4, 8]})

    settings = load_config(path)

    assert settings.ensemble_weights == [2, 4, 8]


def test_missing_yaml_falls_back_to_defaults(tmp_path) -> None:
    """A non-existent config path never raises — defaults are returned."""
    missing = str(tmp_path / "does_not_exist.yaml")

    settings = load_config(missing)

    assert settings == Settings()  # pure defaults, no exception


def test_malformed_yaml_falls_back_to_defaults(tmp_path) -> None:
    """Invalid YAML content is swallowed; the loader yields defaults."""
    bad = tmp_path / "broken.yaml"
    # Unterminated flow mapping — not parseable as YAML.
    bad.write_text("port: 9000\n  : : : oops\n[unclosed", encoding="utf-8")

    settings = load_config(str(bad))

    assert settings == Settings()


def test_non_mapping_yaml_falls_back_to_defaults(write_yaml, tmp_path) -> None:
    """A YAML document that isn't a mapping (e.g. a list) is ignored."""
    not_a_map = tmp_path / "list.yaml"
    not_a_map.write_text("- one\n- two\n", encoding="utf-8")

    settings = load_config(str(not_a_map))

    assert settings == Settings()


def test_unknown_yaml_keys_ignored(write_yaml) -> None:
    """Keys with no matching field are dropped (no crash, no attribute added)."""
    path = write_yaml({"port": 9000, "totally_unknown_key": "value"})

    settings = load_config(path)

    assert settings.port == 9000
    assert not hasattr(settings, "totally_unknown_key")


def test_unknown_env_vars_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """An UPPERCASE env var that maps to no field is ignored entirely."""
    monkeypatch.setenv("SOME_RANDOM_UNRELATED_VAR", "nonsense")

    settings = load_config()

    assert settings == Settings()
    assert not hasattr(settings, "some_random_unrelated_var")


def test_config_path_env_var_resolved(write_yaml, monkeypatch: pytest.MonkeyPatch) -> None:
    """With no explicit arg, ``CONFIG_PATH`` selects the YAML file to load."""
    path = write_yaml({"port": 9222})
    monkeypatch.setenv("CONFIG_PATH", path)

    settings = load_config()  # no explicit path argument

    assert settings.port == 9222


def test_explicit_path_beats_config_path_env(
    write_yaml, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit ``config_path`` argument takes precedence over ``CONFIG_PATH``."""
    explicit = write_yaml({"port": 7000})
    env_path = write_yaml({"port": 8500})
    monkeypatch.setenv("CONFIG_PATH", env_path)

    settings = load_config(explicit)

    assert settings.port == 7000  # explicit arg wins over the env-pointed file


def test_get_config_is_equivalent_to_load_config(
    write_yaml, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``get_config`` resolves identically to ``load_config`` (same precedence)."""
    path = write_yaml({"port": 9000})
    monkeypatch.setenv("PORT", "9300")

    assert get_config(path) == load_config(path)
    assert get_config(path).port == 9300  # env still wins through the alias
