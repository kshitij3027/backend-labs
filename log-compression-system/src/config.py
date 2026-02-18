"""Configuration module — frozen dataclasses loaded from environment variables."""

import os
import argparse
from dataclasses import dataclass


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 5000


def load_server_config() -> ServerConfig:
    return ServerConfig(
        host=os.environ.get("SERVER_HOST", ServerConfig.host),
        port=int(os.environ.get("SERVER_PORT", str(ServerConfig.port))),
    )


@dataclass(frozen=True)
class ClientConfig:
    server_host: str = "localhost"
    server_port: int = 5000
    batch_size: int = 50
    batch_interval: float = 5.0
    compression_enabled: bool = True
    compression_algorithm: str = "gzip"
    compression_level: int = 6
    log_rate: int = 100
    run_time: int = 30
    bypass_threshold: int = 256
    adaptive_enabled: bool = False
    adaptive_min_level: int = 1
    adaptive_max_level: int = 9
    adaptive_check_interval: float = 5.0


def load_client_config(argv=None) -> ClientConfig:
    """Build ClientConfig from env vars, then override with CLI args."""
    # Start with env vars (fall back to dataclass defaults)
    env_server_host = os.environ.get("SERVER_HOST", ClientConfig.server_host)
    env_server_port = int(os.environ.get("SERVER_PORT", str(ClientConfig.server_port)))
    env_batch_size = int(os.environ.get("BATCH_SIZE", str(ClientConfig.batch_size)))
    env_batch_interval = float(os.environ.get("BATCH_INTERVAL", str(ClientConfig.batch_interval)))
    env_compression_enabled = _parse_bool(os.environ.get("COMPRESSION_ENABLED", "true"))
    env_compression_algorithm = os.environ.get("COMPRESSION_ALGORITHM", ClientConfig.compression_algorithm)
    env_compression_level = int(os.environ.get("COMPRESSION_LEVEL", str(ClientConfig.compression_level)))
    env_log_rate = int(os.environ.get("LOG_RATE", str(ClientConfig.log_rate)))
    env_run_time = int(os.environ.get("RUN_TIME", str(ClientConfig.run_time)))
    env_bypass_threshold = int(os.environ.get("BYPASS_THRESHOLD", str(ClientConfig.bypass_threshold)))
    env_adaptive_enabled = _parse_bool(os.environ.get("ADAPTIVE_ENABLED", "false"))
    env_adaptive_min_level = int(os.environ.get("ADAPTIVE_MIN_LEVEL", str(ClientConfig.adaptive_min_level)))
    env_adaptive_max_level = int(os.environ.get("ADAPTIVE_MAX_LEVEL", str(ClientConfig.adaptive_max_level)))
    env_adaptive_check_interval = float(os.environ.get("ADAPTIVE_CHECK_INTERVAL", str(ClientConfig.adaptive_check_interval)))

    parser = argparse.ArgumentParser(description="Log Compression Client")
    parser.add_argument("--server-host", type=str, default=None)
    parser.add_argument("--server-port", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--batch-interval", type=float, default=None)
    parser.add_argument("--compression-algorithm", type=str, default=None)
    parser.add_argument("--compression-level", type=int, default=None)
    parser.add_argument("--log-rate", type=int, default=None)
    parser.add_argument("--run-time", type=int, default=None)
    parser.add_argument("--no-compress", action="store_true", default=False)

    args = parser.parse_args(argv)

    return ClientConfig(
        server_host=args.server_host if args.server_host is not None else env_server_host,
        server_port=args.server_port if args.server_port is not None else env_server_port,
        batch_size=args.batch_size if args.batch_size is not None else env_batch_size,
        batch_interval=args.batch_interval if args.batch_interval is not None else env_batch_interval,
        compression_enabled=not args.no_compress if args.no_compress else env_compression_enabled,
        compression_algorithm=args.compression_algorithm if args.compression_algorithm is not None else env_compression_algorithm,
        compression_level=args.compression_level if args.compression_level is not None else env_compression_level,
        log_rate=args.log_rate if args.log_rate is not None else env_log_rate,
        run_time=args.run_time if args.run_time is not None else env_run_time,
        bypass_threshold=env_bypass_threshold,
        adaptive_enabled=env_adaptive_enabled,
        adaptive_min_level=env_adaptive_min_level,
        adaptive_max_level=env_adaptive_max_level,
        adaptive_check_interval=env_adaptive_check_interval,
    )
