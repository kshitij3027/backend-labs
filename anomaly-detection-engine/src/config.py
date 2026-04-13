"""Environment-variable based configuration for the anomaly detection engine."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_tuple(name: str, default: tuple) -> tuple:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return tuple(float(x.strip()) for x in raw.split(","))


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded from environment variables."""

    flask_port: int = 5000
    zscore_threshold: float = 3.0
    iforest_contamination: float = 0.1
    ensemble_threshold: float = 0.7
    ensemble_weights: tuple = (0.35, 0.40, 0.25)
    window_size: int = 100
    anomaly_rate: float = 0.05
    log_rate: int = 10
    warm_up_size: int = 100
    random_seed: int = 42
    debug: bool = False

    @classmethod
    def from_env(cls) -> Config:
        """Build a Config instance from the current process environment."""
        return cls(
            flask_port=_env_int("FLASK_PORT", 5000),
            zscore_threshold=_env_float("ZSCORE_THRESHOLD", 3.0),
            iforest_contamination=_env_float("IFOREST_CONTAMINATION", 0.1),
            ensemble_threshold=_env_float("ENSEMBLE_THRESHOLD", 0.7),
            ensemble_weights=_env_tuple("ENSEMBLE_WEIGHTS", (0.35, 0.40, 0.25)),
            window_size=_env_int("WINDOW_SIZE", 100),
            anomaly_rate=_env_float("ANOMALY_RATE", 0.05),
            log_rate=_env_int("LOG_RATE", 10),
            warm_up_size=_env_int("WARM_UP_SIZE", 100),
            random_seed=_env_int("RANDOM_SEED", 42),
            debug=_env_bool("DEBUG", False),
        )
