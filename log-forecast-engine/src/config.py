"""Application configuration for the Predictive Log Analytics Engine.

Configuration precedence (lowest to highest):

    Pydantic field defaults  ->  YAML file (config/config.yaml)  ->  environment

Every knob below maps to ``project_requirements.md`` §7 (Configurable Parameters).
Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``).
An optional ``config/config.yaml`` may override any field — it is merged over the
model defaults — and finally environment variables (the uppercased field name) win
over both, courtesy of ``BaseSettings``.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per
process. The loader is defensive: a missing or malformed YAML file never crashes
startup; it simply falls back to model defaults plus environment overrides.

Note on the ensemble: this build ships a **4-model lightweight ensemble** (ARIMA,
exponential smoothing, linear regression, XGBoost). Prophet and LSTM are dropped per
the build plan, so the default weights cover only these four and sum to 1.0.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a hard dependency, guard anyway
    yaml = None  # type: ignore[assignment]


# Candidate YAML locations tried (in order) when no explicit path / CONFIG_PATH is
# given. The repo-relative path supports local `pytest` runs; the absolute one
# matches the path baked into the container image.
_REPO_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "config.yaml"
)
_CONTAINER_CONFIG_PATH = "/app/config/config.yaml"


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, YAML, then environment.

    Field names are snake_case; the corresponding environment variable is the
    uppercased name (pydantic-settings default), e.g. ``api_port`` <- ``API_PORT``.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Identity / server ---
    app_name: str = "log-forecast-engine"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    # --- Persistence / cache ---
    database_url: str = "postgresql+psycopg2://forecast:forecast@postgres:5432/forecast"
    redis_url: str = "redis://redis:6379/0"

    # --- Scheduling cadence (project_requirements.md §7) ---
    prediction_interval_min: int = 5
    retrain_interval_hr: int = 6

    # --- Forecast horizon ---
    default_horizon_min: int = 60
    horizon_min_steps: int = 1
    horizon_max_steps: int = 288

    # --- Training ---
    training_window_days: int = 7

    # --- Confidence / deploy thresholds ---
    high_confidence_threshold: float = 0.85
    medium_confidence_threshold: float = 0.65
    accuracy_deploy_threshold: float = 0.6

    # --- Ensemble member weights (4-model lightweight ensemble; sum to 1.0) ---
    weight_arima: float = 0.3
    weight_exp_smoothing: float = 0.2
    weight_linear: float = 0.2
    weight_xgboost: float = 0.3

    # --- Dashboard ---
    dashboard_poll_interval_sec: int = 30

    @property
    def model_weights(self) -> dict[str, float]:
        """Return the ensemble member weights keyed by model name."""
        return {
            "arima": self.weight_arima,
            "exp_smoothing": self.weight_exp_smoothing,
            "linear": self.weight_linear,
            "xgboost": self.weight_xgboost,
        }


def _resolve_config_path(config_path: str | None) -> str | None:
    """Pick the YAML path to load.

    Order: explicit ``config_path`` arg, then ``CONFIG_PATH`` env var, then the
    container path (``/app/config/config.yaml``) if it exists, then the repo-relative
    path. Returns ``None`` if nothing exists (defaults + env only).
    """
    if config_path:
        return config_path
    env_path = os.environ.get("CONFIG_PATH")
    if env_path:
        return env_path
    for candidate in (_CONTAINER_CONFIG_PATH, _REPO_CONFIG_PATH):
        if os.path.isfile(candidate):
            return candidate
    return None


def _load_yaml(config_path: str | None) -> dict[str, Any]:
    """Parse the YAML file into a flat dict of overrides.

    Returns an empty dict if the path is missing/unreadable/malformed or yaml is
    unavailable — a bad config file must never crash startup.
    """
    if not config_path or yaml is None or not os.path.isfile(config_path):
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except (OSError, yaml.YAMLError):  # type: ignore[union-attr]
        return {}
    return data if isinstance(data, dict) else {}


def load_settings(config_path: str | None = None) -> Settings:
    """Build :class:`Settings` applying defaults -> YAML -> environment precedence.

    YAML values seed the model's kwargs; environment variables (read by
    ``BaseSettings``) still take final precedence over those YAML-supplied values.
    Unknown YAML keys are ignored (``extra="ignore"``).
    """
    overrides = _load_yaml(_resolve_config_path(config_path))
    # Only pass through keys that are real Settings fields so a stray YAML key cannot
    # raise; env vars still override these via BaseSettings resolution order.
    valid = {k: v for k, v in overrides.items() if k in Settings.model_fields}
    return Settings(**valid)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return load_settings()
