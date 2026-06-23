"""Application configuration for the Real-Time Log Pattern Clustering engine.

Configuration precedence (lowest to highest):

    Pydantic model defaults  ->  YAML file  ->  environment variables

Defaults live on the nested :class:`pydantic.BaseModel` config groups (one model per
section: ``kmeans``, ``dbscan``, ``hdbscan``, ``text_features`` ...). A YAML file may
override any field — it is **deep-merged** over the defaults so partial sections are
fine (e.g. setting only ``kmeans.n_clusters`` leaves the other kmeans fields at their
defaults). Finally a small set of ops-knob environment variables (``REDIS_HOST``,
``API_PORT`` ...) win over everything else.

The loader is intentionally defensive: a missing or malformed YAML file must not
crash the application — it simply falls back to model defaults plus any environment
overrides. The nested deep-merge means new YAML knobs are picked up automatically
without touching this loader; only the handful of operational env overrides are
applied explicitly.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

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


class KMeansConfig(BaseModel):
    """K-means clustering hyperparameters (sklearn ``KMeans`` / ``MiniBatchKMeans``)."""

    n_clusters: int = 8
    max_iter: int = 300
    random_state: int = 42


class DBSCANConfig(BaseModel):
    """DBSCAN density clustering hyperparameters."""

    eps: float = 0.3
    min_samples: int = 5


class HDBSCANConfig(BaseModel):
    """HDBSCAN hierarchical-density clustering hyperparameters."""

    min_cluster_size: int = 10
    min_samples: int = 5


class TextFeaturesConfig(BaseModel):
    """TF-IDF text feature extraction settings.

    ``ngram_range`` is stored as a ``list[int]`` (YAML yields a list); use
    :pyattr:`ngram_tuple` when an sklearn-style ``(min_n, max_n)`` tuple is needed.
    """

    max_features: int = 1000
    ngram_range: list[int] = Field(default_factory=lambda: [1, 2])

    @property
    def ngram_tuple(self) -> tuple[int, int]:
        """Return ``ngram_range`` as a 2-tuple suitable for sklearn vectorizers."""
        lo, hi = self.ngram_range[0], self.ngram_range[-1]
        return (lo, hi)


class TemporalFeaturesConfig(BaseModel):
    """Temporal feature extraction settings (sliding-window sizes, in minutes)."""

    time_windows: list[int] = Field(default_factory=lambda: [1, 5, 15, 60])


class BehavioralFeaturesConfig(BaseModel):
    """Behavioral feature extraction settings (request frequency / error rates)."""

    frequency_threshold: float = 0.01


class RealtimeConfig(BaseModel):
    """Streaming engine knobs: micro-batch size, refit cadence, cluster cap."""

    batch_size: int = 100
    update_interval: int = 30
    max_clusters: int = 50


class RedisConfig(BaseModel):
    """Redis connection settings (cluster-state / streaming backend)."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0


class ApiConfig(BaseModel):
    """FastAPI / uvicorn server settings."""

    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = True


class AppConfig(BaseModel):
    """Top-level application config aggregating every nested section.

    Field names map directly to the spec's Configurable Parameters (project
    requirements §7). Construct via :func:`load_config` rather than directly so the
    YAML + environment overrides are applied.
    """

    model_config = ConfigDict(extra="ignore")

    kmeans: KMeansConfig = Field(default_factory=KMeansConfig)
    dbscan: DBSCANConfig = Field(default_factory=DBSCANConfig)
    hdbscan: HDBSCANConfig = Field(default_factory=HDBSCANConfig)
    text_features: TextFeaturesConfig = Field(default_factory=TextFeaturesConfig)
    temporal_features: TemporalFeaturesConfig = Field(
        default_factory=TemporalFeaturesConfig
    )
    behavioral_features: BehavioralFeaturesConfig = Field(
        default_factory=BehavioralFeaturesConfig
    )
    realtime: RealtimeConfig = Field(default_factory=RealtimeConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)


def _resolve_config_path(config_path: str | None) -> str | None:
    """Pick the YAML path to load.

    Order: explicit ``config_path`` arg, then ``CONFIG_PATH`` env var, then the
    container path (``/app/config/config.yaml``) if it exists, then the repo-relative
    path. Returns ``None`` if nothing exists (defaults-only load).
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
    """Parse the YAML file into a nested dict of overrides.

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


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base`` (override wins), returning a new dict.

    Nested dicts are merged key-by-key; any non-dict value (or a type mismatch)
    replaces the base value outright. Inputs are not mutated.
    """
    merged = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _env_overrides() -> dict[str, Any]:
    """Collect operational env-var overrides into a nested dict.

    Only a small set of ops knobs is sourced from the environment; everything else is
    expected to come from YAML. Values are coerced to the appropriate type; malformed
    numeric values are skipped rather than raising.
    """
    redis: dict[str, Any] = {}
    api: dict[str, Any] = {}

    if (v := os.environ.get("REDIS_HOST")) is not None:
        redis["host"] = v
    if (v := os.environ.get("REDIS_PORT")) is not None:
        try:
            redis["port"] = int(v)
        except ValueError:
            pass
    if (v := os.environ.get("REDIS_DB")) is not None:
        try:
            redis["db"] = int(v)
        except ValueError:
            pass

    if (v := os.environ.get("API_HOST")) is not None:
        api["host"] = v
    if (v := os.environ.get("API_PORT")) is not None:
        try:
            api["port"] = int(v)
        except ValueError:
            pass
    if (v := os.environ.get("API_DEBUG")) is not None:
        api["debug"] = v.strip().lower() not in {"", "0", "false", "no", "off"}

    overrides: dict[str, Any] = {}
    if redis:
        overrides["redis"] = redis
    if api:
        overrides["api"] = api
    # LOG_LEVEL is consumed directly by logging/uvicorn setup elsewhere; it is read
    # here only to acknowledge it as a recognized ops knob (no AppConfig field).
    return overrides


def load_config(config_path: str | None = None) -> AppConfig:
    """Load :class:`AppConfig` applying defaults -> YAML -> environment precedence.

    Args:
        config_path: Optional explicit path to a YAML config file. If ``None``, the
            ``CONFIG_PATH`` env var is consulted, then the container/repo default
            locations.

    Returns:
        A fully-populated :class:`AppConfig`. Missing/malformed YAML never raises; it
        simply yields model defaults overlaid with any environment overrides.
    """
    resolved = _resolve_config_path(config_path)

    # Start from model defaults serialized to a plain dict, deep-merge YAML, then env.
    base = AppConfig().model_dump()
    merged = _deep_merge(base, _load_yaml(resolved))
    merged = _deep_merge(merged, _env_overrides())

    return AppConfig.model_validate(merged)


#: Alias for :func:`load_config` for call sites that read more naturally.
get_config = load_config
