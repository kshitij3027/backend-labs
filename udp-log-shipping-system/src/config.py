"""Configuration module — frozen dataclass loaded from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    host: str = "0.0.0.0"
    port: int = 5514
    buffer_size: int = 65536
    log_dir: str = "./logs"
    log_filename: str = "server.log"
    flush_count: int = 100
    flush_timeout_sec: int = 5
    max_errors: int = 100


def load_config() -> Config:
    """Build Config from environment variables with sensible defaults."""
    return Config(
        host=os.environ.get("SERVER_HOST", Config.host),
        port=int(os.environ.get("SERVER_PORT", Config.port)),
        buffer_size=int(os.environ.get("BUFFER_SIZE", Config.buffer_size)),
        log_dir=os.environ.get("LOG_DIR", Config.log_dir),
        log_filename=os.environ.get("LOG_FILENAME", Config.log_filename),
        flush_count=int(os.environ.get("FLUSH_COUNT", Config.flush_count)),
        flush_timeout_sec=int(os.environ.get("FLUSH_TIMEOUT_SEC", Config.flush_timeout_sec)),
        max_errors=int(os.environ.get("MAX_ERRORS", Config.max_errors)),
    )
