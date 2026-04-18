"""Application configuration backed by environment variables.

All tunables live here as a single ``Settings`` object so the rest of
the code imports from one place. Defaults mirror ``.env.example``.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment (or optional .env file)."""

    redis_url: str = "redis://redis:6379"
    db_path: str = "/data/logs.db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Cache TTL for a fully-assembled search/facet response (seconds).
    facet_cache_ttl: int = 30
    # Cache TTL for the distinct-values-per-facet hash (seconds).
    facet_values_ttl: int = 300

    # Max facet values returned per dimension (before "show more").
    max_facet_values: int = 8
    # Default page size for /api/search if client omits ``limit``.
    default_search_limit: int = 10

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


# Module-level singleton. Import as ``from src.config import settings``.
settings = Settings()
