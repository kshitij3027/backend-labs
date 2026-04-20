"""Application configuration backed by environment variables.

All tunables for the real-time log indexing engine live here in a
single ``Settings`` object so the rest of the code imports from one
place. Defaults mirror the table in ``project_requirements.md``
section 7 and ``.env.example``.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment (or optional .env file).

    Every field maps 1:1 to the env-var table in the project
    requirements document. Env names are uppercase (e.g.
    ``BATCH_TIMEOUT_MS``) and ``case_sensitive=False`` means lowercase
    forms also work. Reading a singleton via ``from src.config import
    settings`` is cheap — the instance is constructed once at import
    time.
    """

    # --- Indexing pipeline knobs ------------------------------------
    batch_timeout_ms: int = 100
    segment_max_docs: int = 10_000
    segment_max_memory_mb: int = 50
    max_memory_segments: int = 5

    # --- Redis transport --------------------------------------------
    redis_url: str = "redis://localhost:6379"
    redis_stream_name: str = "logs"
    redis_consumer_group: str = "indexer"

    # --- HTTP server -------------------------------------------------
    http_port: int = 8080

    # --- Segment persistence ----------------------------------------
    disk_segment_dir: str = "./data/segments"
    background_merge_interval_s: int = 30

    # --- Reconnect / backoff ----------------------------------------
    redis_reconnect_backoff_base_s: float = 0.5
    redis_reconnect_backoff_max_s: float = 30.0

    # --- Sample generation ------------------------------------------
    sample_generation_batch_size: int = 500

    # --- Observability ----------------------------------------------
    log_level: str = "INFO"

    # --- WebSocket --------------------------------------------------
    ws_heartbeat_interval_s: int = 20

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# Module-level singleton. Import as ``from src.config import settings``.
settings = Settings()
