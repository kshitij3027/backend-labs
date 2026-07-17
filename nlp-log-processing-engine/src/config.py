"""Application configuration for the NLP Log Processing Engine.

Configuration precedence (lowest to highest):

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``).
This is the standard pydantic-settings source order (environment beats dotenv beats
defaults), so no source customization is needed. Environment variable names are the
upper-cased field names (pydantic-settings default, ``case_sensitive=False``), e.g.
``log_level`` <- ``LOG_LEVEL``.

C1 carries only the handful of settings the scaffold needs. The NLP tunables
(intent-confidence floor, sentiment thresholds, stats window, trending top-k, ...) are
added to this same model in later commits and read off :func:`get_settings` at their
call sites, so operators can retune behaviour purely via environment / ``.env`` without
touching code.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per
process; tests that monkeypatch the environment clear the cache via
``get_settings.cache_clear()``.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, optional .env, then environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Server / logging ---
    #: Root log level for the process (uvicorn / app loggers).
    log_level: str = "INFO"
    #: Informational: the uvicorn bind port inside the container (compose maps
    #: ${BACKEND_PORT} -> 8000). Kept here so the value is discoverable in one place; the
    #: container CMD hard-codes ``--port 8000``.
    backend_port: int = 8000

    # --- Live stream (placeholder) ---
    #: Master switch for the background live-stream loop. Unused in C1 — declared now so
    #: the compose ``test`` service's ``LIVE_STREAM_ENABLED=false`` has a home and the flag
    #: is forward-compatible; the streaming path is wired in a later commit.
    live_stream_enabled: bool = False

    # --- Stats / dashboard (C8) ---
    #: Rolling-window size for the in-memory ``StatsAggregator`` — bounds both the newest-first
    #: ``recent`` buffer and the timestamp window backing ``throughput_per_sec`` on ``/api/stats``.
    stats_window: int = 500
    #: Number of trending keywords ``GET /api/stats`` returns (``StatsAggregator`` top-k).
    trending_top_k: int = 10

    # --- CORS / WebSocket live feed (C9) ---
    #: Origins the CORS middleware allows. Fully permissive (``["*"]``) by default — in
    #: production nginx proxies the dashboard same-origin, so this only relaxes direct
    #: cross-origin access in dev. ``"*"`` anywhere in the list means allow-any, and credentials
    #: are then disabled (the CORS spec forbids pairing the wildcard origin with credentials —
    #: see :func:`src.api.create_app`). ``NoDecode`` + the validator below let operators set
    #: ``CORS_ORIGINS`` as a plain comma-separated string rather than JSON.
    cors_origins: Annotated[list[str], NoDecode] = ["*"]
    #: Master switch for the ``/ws`` live feed. The route and the analyze-broadcast are always
    #: wired; this is declared for forward-compat / operability so the flag has a home and ops
    #: get a documented knob.
    ws_enabled: bool = True

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, value: object) -> object:
        """Accept ``CORS_ORIGINS`` as a comma-separated string, not only a JSON / Python list.

        With ``NoDecode`` the environment value reaches this validator as a raw string (pydantic
        -settings skips its usual JSON decode for the field), so we split on commas and trim each
        origin — ``CORS_ORIGINS=http://a, http://b`` and ``CORS_ORIGINS=*`` both just work. A real
        list (the Python default, or one supplied from code) is passed straight through untouched.
        """
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return Settings()
