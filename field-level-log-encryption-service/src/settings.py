"""Application settings loaded from environment variables / .env via pydantic-settings.

A module-level `settings` singleton is exposed so other modules can simply
`from src.settings import settings`. Adding new config knobs only requires
extending the `Settings` model below.
"""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the field-level log encryption service.

    All values come from environment variables (case-insensitive) or the
    `.env` file at the project root. The single required value is
    `MASTER_KEY_B64` — the base64-encoded 32-byte KEK used to wrap DEKs.
    """

    # --- Web / server ---
    port: int = 8000
    log_level: str = "INFO"

    # --- Key management ---
    # Required: base64-encoded 32-byte KEK. No default — production deployments
    # must provide their own. For local dev see `.env.example`.
    master_key_b64: str = Field(..., description="Base64-encoded 32-byte KEK")
    key_rotation_days: int = 30

    # --- Performance / batching ---
    batch_parallel_threshold_fields: int = 4
    batch_parallel_threshold_bytes: int = 4096
    thread_pool_size: int = 4

    # --- Cache (Redis added in C9) ---
    redis_host: str = "redis"
    redis_port: int = 6379

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# Module-level singleton — import this everywhere instead of constructing Settings ad-hoc.
settings = Settings()
