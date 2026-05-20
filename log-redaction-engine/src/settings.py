"""Application settings loaded from environment variables / ``.env``.

This module exposes a ``Settings`` model and an LRU-cached ``get_settings()``
accessor. The caching matters because pydantic-settings reads the ``.env``
file on every ``Settings()`` instantiation; cheaper to read once and hand
out the same singleton on every subsequent call.

Required vs default fields
--------------------------
Most knobs have sensible defaults so the service runs cleanly in development
without any env setup. The one exception is ``REDACTION_HASH_SALT``: it is
required (no default) because hash-based redaction must be deterministic
across restarts within a deployment but must NOT be guessable across
deployments. Forcing the operator to supply it explicitly prevents the
"oops, we used the in-repo default in production" failure mode.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the log redaction engine.

    All fields are populated from environment variables (case-insensitive)
    or the ``.env`` file at the project root. See ``.env.example`` for the
    canonical list of supported variables.
    """

    # --- Web / server ---
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    # --- Redaction core ---
    REDACTION_PRESET: str = "general"
    # REQUIRED — no default. Used to keep hash-based redactions deterministic
    # within a deployment while making them unguessable across deployments.
    # Generate via: python -c 'import secrets; print(secrets.token_hex(32))'
    REDACTION_HASH_SALT: str = Field(..., description="Hex salt for hash-based redaction")

    # --- Named entity recognition (spaCy en_core_web_sm) ---
    # Minimum input length (chars) before NER runs. Short fragments rarely
    # contain person/org names worth detecting and NER is the slowest stage,
    # so this is the cheapest place to filter.
    NER_MIN_LENGTH: int = 40
    NER_ENABLED: bool = True

    # --- Observability / metrics ---
    AUDIT_BUFFER_SIZE: int = 10000
    STATS_WINDOW_SECONDS: int = 60

    # --- Performance / batching ---
    MAX_TOKEN_COUNT: int = 100000
    BATCH_PARALLEL_THRESHOLD: int = 50
    THREAD_POOL_SIZE: int = 4
    # Per-pattern regex evaluation budget. Catastrophic backtracking on
    # adversarial input is bounded by this timeout.
    REGEX_TIMEOUT_SEC: float = 0.05

    # --- Cache (Redis is wired in C10) ---
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_ENABLED: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide ``Settings`` singleton.

    LRU-cached so the ``.env`` file is parsed exactly once per process.
    Tests that need a fresh instance can call ``get_settings.cache_clear()``.
    """
    return Settings()
