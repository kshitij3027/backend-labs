from functools import lru_cache
import base64
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Env-driven configuration. .env file is loaded automatically."""

    # --- Server ---
    port: int = 8000
    log_level: str = "INFO"

    # --- Persistence ---
    database_url: str = "sqlite+aiosqlite:////app/data/audit.db"

    # --- Crypto ---
    # Base64-encoded 32-byte Ed25519 seed. No default — fail fast at startup.
    signing_key_b64: str = Field(..., description="Base64 of a 32-byte Ed25519 seed")

    # --- Chain ---
    chain_genesis_note: str = "immutable-audit-trail"

    # --- Reports ---
    report_default_range_days: int = 30

    # --- Dashboard ---
    dashboard_refresh_ms: int = 10000

    # --- Auth headers ---
    anonymous_user_id: str = "anonymous"
    user_header_name: str = "X-User-ID"
    session_header_name: str = "X-Session-ID"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("signing_key_b64")
    @classmethod
    def _signing_key_must_be_32_bytes(cls, v: str) -> str:
        try:
            raw = base64.b64decode(v, validate=True)
        except Exception as exc:
            raise ValueError(f"SIGNING_KEY_B64 is not valid base64: {exc}") from exc
        if len(raw) != 32:
            raise ValueError(
                f"SIGNING_KEY_B64 must decode to exactly 32 bytes, got {len(raw)}"
            )
        return v


@lru_cache
def get_settings() -> Settings:
    """Cached settings accessor — read .env once per process."""
    return Settings()  # type: ignore[call-arg]
