"""Configuration module — frozen dataclasses loaded from environment variables."""

import os
import argparse
from dataclasses import dataclass


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 9999
    buffer_size: int = 65535


def load_server_config() -> ServerConfig:
    """Build ServerConfig from environment variables with sensible defaults."""
    return ServerConfig(
        host=os.environ.get("SERVER_HOST", ServerConfig.host),
        port=int(os.environ.get("SERVER_PORT", ServerConfig.port)),
        buffer_size=int(os.environ.get("BUFFER_SIZE", ServerConfig.buffer_size)),
    )


@dataclass(frozen=True)
class ClientConfig:
    target_host: str = "localhost"
    target_port: int = 9999
    batch_size: int = 10
    flush_interval: float = 5.0
    compress: bool = True
    max_retries: int = 3
    logs_per_second: int = 5
    run_time: int = 30


def load_client_config(argv=None) -> ClientConfig:
    """Build ClientConfig from environment variables, then override with CLI args.

    Pass argv for testability; when None, argparse reads sys.argv.
    """
    # Start with env-var values (falling back to dataclass defaults)
    env_target_host = os.environ.get("TARGET_HOST", ClientConfig.target_host)
    env_target_port = int(os.environ.get("TARGET_PORT", ClientConfig.target_port))
    env_batch_size = int(os.environ.get("BATCH_SIZE", ClientConfig.batch_size))
    env_flush_interval = float(
        os.environ.get("FLUSH_INTERVAL", ClientConfig.flush_interval)
    )
    env_compress = _parse_bool(os.environ.get("COMPRESS", "true"))
    env_max_retries = int(os.environ.get("MAX_RETRIES", ClientConfig.max_retries))
    env_logs_per_second = int(
        os.environ.get("LOGS_PER_SECOND", ClientConfig.logs_per_second)
    )
    env_run_time = int(os.environ.get("RUN_TIME", ClientConfig.run_time))

    # CLI flags override env vars
    parser = argparse.ArgumentParser(description="Batch Log Shipper Client")
    parser.add_argument("--target-host", type=str, default=None)
    parser.add_argument("--target-port", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--batch-interval", type=float, default=None)
    parser.add_argument("--logs-per-second", type=int, default=None)
    parser.add_argument("--run-time", type=int, default=None)
    parser.add_argument("--no-compress", action="store_true", default=False)

    args = parser.parse_args(argv)

    return ClientConfig(
        target_host=args.target_host if args.target_host is not None else env_target_host,
        target_port=args.target_port if args.target_port is not None else env_target_port,
        batch_size=args.batch_size if args.batch_size is not None else env_batch_size,
        flush_interval=args.batch_interval if args.batch_interval is not None else env_flush_interval,
        compress=not args.no_compress if args.no_compress else env_compress,
        max_retries=env_max_retries,
        logs_per_second=args.logs_per_second if args.logs_per_second is not None else env_logs_per_second,
        run_time=args.run_time if args.run_time is not None else env_run_time,
    )
