"""Configuration module — frozen dataclass loaded from environment variables."""

import os
from dataclasses import dataclass

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class Config:
    host: str = "0.0.0.0"
    port: int = 9000
    buffer_size: int = 4096
    min_log_level: str = "INFO"
    enable_log_persistence: bool = True
    log_dir: str = "./logs"
    log_filename: str = "server.log"
    rate_limit_enabled: bool = True
    rate_limit_max_requests: int = 100
    rate_limit_window_seconds: int = 60


def load_config() -> Config:
    """Build Config from environment variables with sensible defaults."""
    return Config(
        host=os.environ.get("SERVER_HOST", Config.host),
        port=int(os.environ.get("SERVER_PORT", Config.port)),
        buffer_size=int(os.environ.get("BUFFER_SIZE", Config.buffer_size)),
        min_log_level=os.environ.get("MIN_LOG_LEVEL", Config.min_log_level).upper(),
        enable_log_persistence=_parse_bool(
            os.environ.get("ENABLE_LOG_PERSISTENCE", "true")
        ),
        log_dir=os.environ.get("LOG_DIR", Config.log_dir),
        log_filename=os.environ.get("LOG_FILENAME", Config.log_filename),
        rate_limit_enabled=_parse_bool(
            os.environ.get("RATE_LIMIT_ENABLED", "true")
        ),
        rate_limit_max_requests=int(
            os.environ.get("RATE_LIMIT_MAX_REQUESTS", Config.rate_limit_max_requests)
        ),
        rate_limit_window_seconds=int(
            os.environ.get("RATE_LIMIT_WINDOW_SECONDS", Config.rate_limit_window_seconds)
        ),
    )
