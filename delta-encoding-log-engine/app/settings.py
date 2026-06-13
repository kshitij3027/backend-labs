"""Runtime configuration for the delta-encoding log storage engine.

Every field is overridable via an environment variable of the same name
(UPPER_SNAKE, case-insensitive) or a project-root ``.env`` file. ``.env.example``
documents the full set with defaults, and the field names/defaults here mirror
it one-for-one.

The class is deliberately flat: a single :func:`get_settings` call is the one
source of configuration truth for the API, the synthetic generator, the pattern
analyzer, the reconstruction cache, and the dashboard.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide engine configuration, resolved once via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- API / server ---
    api_host: str = "0.0.0.0"
    """Interface uvicorn binds to. 0.0.0.0 so the port is reachable in Docker."""

    api_port: int = Field(8080, ge=1, le=65535)
    """TCP port of the API + dashboard (also the host port in compose)."""

    # --- delta encoding ---
    keyframe_interval: int = Field(100, ge=1)
    """Emit a full keyframe every N entries (storage <-> random-access dial)."""

    delta_baseline: Literal["previous", "keyframe"] = "previous"
    """Diff each entry against the ``previous`` entry or the segment ``keyframe``."""

    gzip_deltas: bool = False
    """Also gzip the delta stream (delta encoding composes with byte compression)."""

    # --- synthetic log generator ---
    generator_field_churn: float = Field(0.2, ge=0.0, le=1.0)
    """Fraction of fields that change between generated entries (0.0..1.0)."""

    generator_schema_width: int = Field(8, ge=1)
    """Number of fields per generated entry."""

    # --- pattern analyzer ---
    analyzer_window: int = 200
    """Sliding-window size for the pattern analyzer."""

    # --- reconstruction ---
    reconstruct_cache_size: int = Field(1024, ge=0)
    """LRU cache size for reconstructed entries (0 disables caching)."""

    # --- dashboard ---
    dashboard_refresh_ms: int = Field(2000, ge=100)
    """Dashboard websocket tick cadence in milliseconds."""

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
