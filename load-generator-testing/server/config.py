"""Server configuration loaded from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServerConfig:
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 9000
    BUFFER_SIZE: int = 65536
    ENABLE_TLS: bool = False
    CERT_DIR: str = "./certs"
    ENABLE_UDP: bool = False
    UDP_PORT: int = 9001
    ENABLE_PERSISTENCE: bool = True
    LOG_DIR: str = "./logs"
    MIN_LOG_LEVEL: str = "DEBUG"
    CIRCUIT_BREAKER_ENABLED: bool = False
    BATCH_SIZE: int = 500
    BATCH_FLUSH_MS: int = 100

    @classmethod
    def from_env(cls) -> "ServerConfig":
        """Create a ServerConfig from environment variables with defaults."""

        def _parse_bool(value: str) -> bool:
            return value.lower() in ("true", "1", "yes")

        return cls(
            SERVER_HOST=os.environ.get("SERVER_HOST", "0.0.0.0"),
            SERVER_PORT=int(os.environ.get("SERVER_PORT", "9000")),
            BUFFER_SIZE=int(os.environ.get("BUFFER_SIZE", "65536")),
            ENABLE_TLS=_parse_bool(os.environ.get("ENABLE_TLS", "false")),
            CERT_DIR=os.environ.get("CERT_DIR", "./certs"),
            ENABLE_UDP=_parse_bool(os.environ.get("ENABLE_UDP", "false")),
            UDP_PORT=int(os.environ.get("UDP_PORT", "9001")),
            ENABLE_PERSISTENCE=_parse_bool(
                os.environ.get("ENABLE_PERSISTENCE", "true")
            ),
            LOG_DIR=os.environ.get("LOG_DIR", "./logs"),
            MIN_LOG_LEVEL=os.environ.get("MIN_LOG_LEVEL", "DEBUG"),
            CIRCUIT_BREAKER_ENABLED=_parse_bool(
                os.environ.get("CIRCUIT_BREAKER_ENABLED", "false")
            ),
            BATCH_SIZE=int(os.environ.get("BATCH_SIZE", "500")),
            BATCH_FLUSH_MS=int(os.environ.get("BATCH_FLUSH_MS", "100")),
        )
