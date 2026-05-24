"""Env-driven configuration loaded once per process via lru_cache.

All values have safe defaults so the app starts on a fresh clone with
no ``.env`` file. Overrides come from ``.env`` (loaded automatically by
pydantic-settings) or real environment variables — env vars win.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings."""

    # --- Server ---
    port: int = 8000
    log_level: str = "INFO"

    # --- Persistence ---
    database_url: str = "sqlite+aiosqlite:///data/retention.db"

    # --- Storage tiers ---
    storage_root: str = "/app/data/tiers"

    # --- Policy config ---
    policy_config_path: str = "config/retention_config.yaml"

    # --- Dashboard ---
    dashboard_refresh_ms: int = 5000

    # --- Scheduler intervals (seconds) ---
    scan_interval_sec: int = 60
    apply_interval_sec: int = 60
    sweep_interval_sec: int = 60

    # --- Delete delay (mark-then-sweep) ---
    delete_delay_hours: int = 24

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor — read ``.env`` once per process."""
    return Settings()  # type: ignore[call-arg]
