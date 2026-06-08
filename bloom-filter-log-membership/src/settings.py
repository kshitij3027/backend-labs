"""Runtime configuration for the bloom-filter log membership service.

Every field is overridable via an environment variable of the same name
(UPPER_SNAKE, case-insensitive) or a project-root ``.env`` file. ``.env.example``
documents the full set with defaults.

The class is deliberately flat and easy to extend: later commits append the
per-log-type filter sizing fields (capacity / target FP rate per filter),
snapshot + rotation intervals, scalable-filter growth parameters, and the
two-tier pipeline thresholds — all as plain defaulted fields here, so one
``get_settings()`` call stays the single source of configuration truth.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide service configuration, resolved once via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- API / server ---
    api_host: str = "0.0.0.0"
    """Interface uvicorn binds to. 0.0.0.0 so the port is reachable in Docker."""

    api_port: int = 8001
    """TCP port of the membership API (the dashboard process later takes 8002)."""

    # --- storage ---
    data_dir: str = "./data"
    """Directory holding persisted filter snapshots (``*.bloom``).

    Compose sets this to ``/app/data`` and bind-mounts ``./data`` over it so
    filters survive container restarts; tests point it at a tmp dir.
    """

    # --- logging ---
    log_level: str = "INFO"
    """Stdlib logging level name (DEBUG / INFO / WARNING / ERROR)."""


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    The LRU cache makes this a cheap singleton accessor for request handlers
    and background tasks. Tests override env vars and call
    ``get_settings.cache_clear()`` to force a rebuild.
    """
    return Settings()
