"""Application configuration. Loaded once at import time."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings. JWT_SECRET_KEY is required — service refuses to start without it."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    jwt_secret_key: str = Field(..., min_length=8, description="HMAC key for JWT signing — must be set via env")
    jwt_algorithm: str = Field(default="HS256")
    jwt_expiry_minutes: int = Field(default=60, ge=1, le=1440)

    app_host: str = Field(default="0.0.0.0")
    app_port: int = Field(default=8000)
    app_log_level: str = Field(default="info")

    cors_allowed_origins: str = Field(default="http://localhost:3000")

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance. Raises RuntimeError if JWT_SECRET_KEY missing."""
    try:
        return Settings()  # type: ignore[call-arg]
    except Exception as exc:
        # Pydantic raises ValidationError for missing required fields; convert to RuntimeError
        # per the requirements doc ("service fails fast if secret is missing").
        raise RuntimeError(f"Configuration error: {exc}") from exc
