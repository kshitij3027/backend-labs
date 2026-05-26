"""Env-driven configuration loaded once per process via ``lru_cache``.

Every value matches the section 7 Configurable Parameters table in
``project_requirements.md`` plus a few keys the implementation plan
requires (``DATABASE_URL``, the HMAC signing keys, the Fernet
encryption key). Defaults are safe for local Docker Compose; production
deployments must override ``HMAC_SIGNING_KEY`` /
``HMAC_SIGNING_KEY_SECONDARY`` and supply a persistent
``FERNET_ENCRYPTION_KEY``.

List-typed env vars (``SUPPORTED_FRAMEWORKS``, ``EMAIL_RECIPIENTS``)
are stored as comma-separated strings and exposed via ``@property``
helpers so the operator can wire them with the standard env-var syntax.
"""
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Persistence ---
    database_url: str = "postgresql+asyncpg://compliance:compliance@postgres:5432/compliance"
    storage_path: str = "./exports"

    # --- Server ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    frontend_port: int = 3000

    # --- Cryptography ---
    hmac_signing_key: str = "change-me-in-production-this-is-a-32byte-min-key"
    hmac_signing_key_secondary: str = "change-me-in-production-secondary-32byte-key!!"
    fernet_encryption_key: str = ""

    # --- Framework configuration ---
    supported_frameworks: str = "SOX,HIPAA,PCI_DSS,GDPR"

    # --- Report execution ---
    max_concurrent_reports: int = 5
    report_timeout_seconds: int = 300
    default_export_format: str = "PDF"

    # --- Scheduler ---
    scheduler_enabled: bool = False
    schedule_interval: str = "daily"

    # --- Email distribution ---
    email_smtp_host: str = ""
    email_recipients: str = ""

    # --- Observability ---
    log_level: str = "INFO"

    # --- Retention policy ---
    data_retention_days_sox: int = 2555
    data_retention_days_hipaa: int = 2190
    data_retention_days_pci: int = 365
    data_retention_days_gdpr: int = 1095

    # --- Dashboard ---
    dashboard_refresh_ms: int = 5000

    @field_validator("hmac_signing_key")
    @classmethod
    def _hmac_key_must_be_long_enough(cls, v: str) -> str:
        if len(v.strip()) < 32:
            raise ValueError("HMAC_SIGNING_KEY must be at least 32 characters long")
        return v

    @property
    def supported_frameworks_list(self) -> list[str]:
        return [f.strip() for f in self.supported_frameworks.split(",") if f.strip()]

    @property
    def email_recipients_list(self) -> list[str]:
        return [r.strip() for r in self.email_recipients.split(",") if r.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
