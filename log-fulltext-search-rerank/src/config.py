"""Application configuration for the full-text log search + reranker.

Every tunable the service exposes lives on :class:`Settings`. The
object is read from environment variables (and an optional ``.env``
file) via ``pydantic-settings`` so the container, the test-runner, and
the host all load defaults from the same place.

Access the singleton via :func:`get_settings`, which caches via
``functools.lru_cache``. Tests reach for :func:`reset_settings_cache`
between envvar swaps so each test sees a freshly-read :class:`Settings`.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment (or optional .env file).

    Fields are grouped by concern so the surface stays legible even as
    the service grows. Defaults mirror the values referenced in
    ``plan.md`` §5 (Commit 02) and ``project_requirements.md`` §7 so a
    clean environment yields the behaviour documented there.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Server -----------------------------------------------------
    http_host: str = "0.0.0.0"
    http_port: int = 8000
    log_level: Literal["DEBUG", "INFO", "WARN", "WARNING", "ERROR"] = "INFO"

    # --- Tokenizer --------------------------------------------------
    min_token_length: int = 3
    stop_words_language: str = "english"

    # --- Temporal decay (half-life in seconds) ----------------------
    temporal_half_life_normal_s: int = 21600   # 6h
    temporal_half_life_incident_s: int = 900   # 15min

    # --- Ranking weights --------------------------------------------
    severity_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "FATAL": 1.0,
            "ERROR": 1.0,
            "WARN": 0.7,
            "WARNING": 0.7,
            "INFO": 0.4,
            "DEBUG": 0.2,
        }
    )
    ranking_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "tfidf": 0.45,
            "temporal": 0.25,
            "severity": 0.15,
            "service": 0.10,
            "context": 0.05,
        }
    )
    incident_ranking_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "tfidf": 0.30,
            "temporal": 0.35,
            "severity": 0.25,
            "service": 0.05,
            "context": 0.05,
        }
    )
    service_authority_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "payment": 1.0,
            "auth": 0.9,
            "api": 0.8,
            "gateway": 0.8,
            "worker": 0.6,
            "unknown": 0.5,
        }
    )

    # --- Query + search ---------------------------------------------
    default_limit: int = 10
    query_cache_size: int = 1000
    candidate_top_k: int = 200

    # --- Index maintenance ------------------------------------------
    idf_rebuild_every_n_docs: int = 500
    idf_rebuild_every_s: float = 2.0

    # --- Synonyms / intent ------------------------------------------
    synonyms_path: str | None = None
    intent_patterns_path: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide :class:`Settings` instance.

    The ``lru_cache`` means constructing :class:`Settings` only pays the
    env parse cost once per process. Tests that mutate env vars call
    :func:`reset_settings_cache` between assertions so the next
    ``get_settings()`` re-reads the environment.
    """
    return Settings()


def reset_settings_cache() -> None:
    """Clear the cached :class:`Settings` instance.

    Used by test fixtures that need to flip environment variables and
    observe the change on the next :func:`get_settings` call. Safe to
    invoke repeatedly.
    """
    get_settings.cache_clear()
