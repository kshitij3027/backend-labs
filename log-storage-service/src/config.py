"""Configuration module — frozen dataclass loaded from environment variables."""

import os
from dataclasses import dataclass


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class Config:
    log_dir: str = "./logs"
    log_filename: str = "application.log"
    max_file_size_bytes: int = 10 * 1024 * 1024  # 10 MB
    rotation_interval_seconds: int = 3600
    max_file_count: int = 10
    max_age_days: int = 7
    compression_enabled: bool = True


def load_config() -> Config:
    """Build Config from environment variables with sensible defaults."""
    # MAX_FILE_SIZE_BYTES takes precedence over MAX_FILE_SIZE_MB
    raw_bytes = os.environ.get("MAX_FILE_SIZE_BYTES")
    raw_mb = os.environ.get("MAX_FILE_SIZE_MB")
    if raw_bytes is not None:
        max_size = int(raw_bytes)
    elif raw_mb is not None:
        max_size = int(float(raw_mb) * 1024 * 1024)
    else:
        max_size = Config.max_file_size_bytes

    return Config(
        log_dir=os.environ.get("LOG_DIR", Config.log_dir),
        log_filename=os.environ.get("LOG_FILENAME", Config.log_filename),
        max_file_size_bytes=max_size,
        rotation_interval_seconds=int(
            os.environ.get("ROTATION_INTERVAL_SECONDS", Config.rotation_interval_seconds)
        ),
        max_file_count=int(
            os.environ.get("MAX_FILE_COUNT", Config.max_file_count)
        ),
        max_age_days=int(
            os.environ.get("MAX_AGE_DAYS", Config.max_age_days)
        ),
        compression_enabled=_parse_bool(
            os.environ.get("COMPRESSION_ENABLED", "true")
        ),
    )
