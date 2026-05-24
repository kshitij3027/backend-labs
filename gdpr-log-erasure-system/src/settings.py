"""Env-driven configuration loaded once per process via ``lru_cache``.

Every value matches the §7 Configurable Parameters table in
``project_requirements.md`` so the operator can wire the whole system
with environment variables alone. Defaults are safe for local Docker
Compose; production deployments must override
``ANONYMIZATION_HASH_SALT`` and tighten ``CORS_ALLOWED_ORIGINS``.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://erasure_user:changeme@postgres:5432/gdpr_erasure"
    redis_url: str = "redis://redis:6379/0"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    anonymizable_data_types: str = "analytics_events,performance_metrics,system_logs,aggregated_data"
    anonymization_hash_salt: str = "change-me-in-production"
    max_parallel_location_erasures: int = 10
    erasure_retry_count: int = 3
    erasure_retry_backoff_seconds: int = 2
    cors_allowed_origins: str = "*"
    log_level: str = "INFO"
    verification_enabled: bool = True
    dashboard_refresh_ms: int = 5000

    @property
    def anonymizable_data_types_set(self) -> set[str]:
        return {t.strip() for t in self.anonymizable_data_types.split(",") if t.strip()}

    @property
    def cors_origins_list(self) -> list[str]:
        if self.cors_allowed_origins.strip() == "*":
            return ["*"]
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
