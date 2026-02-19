"""Configuration module — frozen dataclass loaded from environment variables."""
import os
from dataclasses import dataclass

def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")

@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8443
    cert_file: str = "/app/certs/server.crt"
    key_file: str = "/app/certs/server.key"
    log_dir: str = "./logs"
    max_logs_per_file: int = 10

@dataclass(frozen=True)
class ClientConfig:
    host: str = "tls-server"
    port: int = 8443
    verify_certs: bool = False
    ca_file: str = ""
    retry_attempts: int = 3
    retry_base_delay: float = 1.0

def load_server_config() -> ServerConfig:
    return ServerConfig(
        host=os.environ.get("SERVER_HOST", ServerConfig.host),
        port=int(os.environ.get("SERVER_PORT", str(ServerConfig.port))),
        cert_file=os.environ.get("CERT_FILE", ServerConfig.cert_file),
        key_file=os.environ.get("KEY_FILE", ServerConfig.key_file),
        log_dir=os.environ.get("LOG_DIR", ServerConfig.log_dir),
        max_logs_per_file=int(os.environ.get("MAX_LOGS_PER_FILE", str(ServerConfig.max_logs_per_file))),
    )

def load_client_config() -> ClientConfig:
    return ClientConfig(
        host=os.environ.get("SERVER_HOST", ClientConfig.host),
        port=int(os.environ.get("SERVER_PORT", str(ClientConfig.port))),
        verify_certs=_parse_bool(os.environ.get("VERIFY_CERTS", "false")),
        ca_file=os.environ.get("CA_FILE", ClientConfig.ca_file),
        retry_attempts=int(os.environ.get("RETRY_ATTEMPTS", str(ClientConfig.retry_attempts))),
        retry_base_delay=float(os.environ.get("RETRY_BASE_DELAY", str(ClientConfig.retry_base_delay))),
    )
