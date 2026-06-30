"""Application configuration for the Log Recommendation Engine.

Configuration precedence (lowest to highest):

    field defaults  ->  YAML file (config/config.yaml)  ->  .env  ->  environment

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``).
An optional ``config/config.yaml`` may override any field — it is merged over the
model defaults — and finally environment variables (the uppercased field name) win
over both.

This precedence is enforced via pydantic-settings v2 source customization
(:meth:`Settings.settings_customise_sources`): the source tuple is ordered so that
init kwargs > env > dotenv > YAML > file secrets. This is the correct fix for the
classic trap where passing YAML values as explicit ``Settings(**yaml)`` constructor
kwargs gives them the *highest* priority and silently shadows environment variables.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per
process. The loader is defensive: a missing or malformed YAML file never crashes
startup; it simply falls back to model defaults plus environment overrides.
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


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
    app_name: str = "log-recommendation-engine"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    # --- Persistence / cache ---
    database_url: str = "postgresql+psycopg2://recommend:recommend@postgres:5432/recommend"
    redis_url: str = "redis://redis:6379/0"

    # --- Embeddings ---
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384
    embedding_cache_ttl_sec: int = 86400  # Redis embedding-cache TTL (24h)

    # --- Retrieval / ranking ---
    top_k: int = 5
    candidate_k: int = 50
    weight_semantic: float = 0.6
    weight_contextual: float = 0.4
    weight_feedback: float = 0.2

    # --- Contextual signal weights ---
    ctx_weight_service: float = 0.4
    ctx_weight_severity: float = 0.2
    ctx_weight_tags: float = 0.25
    ctx_weight_recency: float = 0.15
    recency_half_life_days: float = 30.0

    # --- Feedback loop / exploration / diversity ---
    epsilon_explore: float = 0.1
    diversity_threshold: float = 0.9
    feedback_smoothing: float = 2.0

    # --- Confidence thresholds ---
    high_confidence_threshold: float = 0.75
    medium_confidence_threshold: float = 0.5

    # --- Dashboard ---
    dashboard_poll_interval_sec: int = 15

    @property
    def blend_weights(self) -> dict[str, float]:
        """Return the top-level blend weights keyed by signal name."""
        return {
            "semantic": self.weight_semantic,
            "contextual": self.weight_contextual,
            "feedback": self.weight_feedback,
        }

    @property
    def contextual_weights(self) -> dict[str, float]:
        """Return the contextual sub-signal weights keyed by signal name."""
        return {
            "service": self.ctx_weight_service,
            "severity": self.ctx_weight_severity,
            "tags": self.ctx_weight_tags,
            "recency": self.ctx_weight_recency,
        }

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Order config sources so env vars beat YAML (the precedence contract).

        Highest priority first: init kwargs > environment > .env > YAML > secrets.
        Sources earlier in the returned tuple win, so the YAML source sits *below*
        ``env_settings``/``dotenv_settings`` — this is what makes ``REDIS_URL``,
        ``DATABASE_URL``, ``API_PORT``, etc. on the compose services override the
        values in ``config/config.yaml`` instead of being silently shadowed.

        The YAML path is resolved at construction time (see ``load_settings``) and
        threaded through via the private ``_yaml_path`` init kwarg so explicit paths,
        ``CONFIG_PATH``, and the container/repo fallbacks all still work. A missing
        file is tolerated: ``YamlConfigSettingsSource`` yields an empty mapping rather
        than raising when ``yaml_file`` does not exist.
        """
        yaml_path = None
        # ``init_settings`` holds the kwargs passed to ``Settings(...)``. Pull our
        # private path hint out of it without exposing it as a real model field.
        init_kwargs = getattr(init_settings, "init_kwargs", None)
        if isinstance(init_kwargs, dict):
            yaml_path = init_kwargs.get("_yaml_path")

        yaml_source = YamlConfigSettingsSource(settings_cls, yaml_file=yaml_path)
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
            file_secret_settings,
        )


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


def load_settings(config_path: str | None = None) -> Settings:
    """Build :class:`Settings` with defaults -> YAML -> .env -> environment precedence.

    The YAML file is loaded by a dedicated pydantic-settings source ranked *below*
    the environment sources (see :meth:`Settings.settings_customise_sources`), so
    environment variables take final precedence over YAML — the documented contract.
    The resolved YAML path is threaded through via the private ``_yaml_path`` init
    kwarg; it is not a model field (``extra="ignore"`` drops it). A missing config
    file is tolerated and simply contributes nothing.
    """
    yaml_path = _resolve_config_path(config_path)
    return Settings(_yaml_path=yaml_path)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return load_settings()
