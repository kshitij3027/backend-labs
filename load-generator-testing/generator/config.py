import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    SERVER_HOST: str = "localhost"
    SERVER_PORT: int = 9000
    TOTAL_LOGS: int = 1000
    DURATION_SECS: int = 10
    CONCURRENCY: int = 5
    ENABLE_TLS: bool = False
    CA_CERT_PATH: str = "./certs/ca.crt"
    COMPRESS: bool = False
    BATCH_SIZE: int = 100
    TARGET_RPS: int = 0  # 0 = unlimited

    @classmethod
    def from_env(cls) -> "GeneratorConfig":
        def _parse_bool(value: str) -> bool:
            return value.lower() in ("true", "1", "yes")

        return cls(
            SERVER_HOST=os.environ.get("SERVER_HOST", "localhost"),
            SERVER_PORT=int(os.environ.get("SERVER_PORT", "9000")),
            TOTAL_LOGS=int(os.environ.get("TOTAL_LOGS", "1000")),
            DURATION_SECS=int(os.environ.get("DURATION_SECS", "10")),
            CONCURRENCY=int(os.environ.get("CONCURRENCY", "5")),
            ENABLE_TLS=_parse_bool(os.environ.get("ENABLE_TLS", "false")),
            CA_CERT_PATH=os.environ.get("CA_CERT_PATH", "./certs/ca.crt"),
            COMPRESS=_parse_bool(os.environ.get("COMPRESS", "false")),
            BATCH_SIZE=int(os.environ.get("BATCH_SIZE", "100")),
            TARGET_RPS=int(os.environ.get("TARGET_RPS", "0")),
        )
