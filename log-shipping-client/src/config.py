"""Configuration module — frozen dataclass loaded from env vars and CLI args."""

import os
import sys
from dataclasses import dataclass


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("true", "1", "yes")


@dataclass(frozen=True)
class Config:
    log_file: str = "/var/log/app.log"
    server_host: str = "localhost"
    server_port: int = 9000
    batch_mode: bool = True
    compress: bool = False
    batch_size: int = 1
    metrics_interval: int = 0
    poll_interval: float = 0.5
    buffer_size: int = 50000
    resilient: bool = False


def load_config(argv: list[str] | None = None) -> Config:
    """Build Config from defaults <- env vars <- CLI args (highest priority)."""
    if argv is None:
        argv = sys.argv[1:]

    # Start with env var overrides on top of defaults
    env_mode = os.environ.get("SHIPPING_MODE", "batch")
    kwargs: dict = {
        "log_file": os.environ.get("LOG_FILE", Config.log_file),
        "server_host": os.environ.get("SERVER_HOST", Config.server_host),
        "server_port": int(os.environ.get("SERVER_PORT", str(Config.server_port))),
        "batch_mode": env_mode.lower() == "batch",
        "compress": _parse_bool(os.environ.get("COMPRESS", "false")),
        "batch_size": int(os.environ.get("BATCH_SIZE", str(Config.batch_size))),
        "metrics_interval": int(
            os.environ.get("METRICS_INTERVAL", str(Config.metrics_interval))
        ),
        "poll_interval": float(
            os.environ.get("POLL_INTERVAL", str(Config.poll_interval))
        ),
        "buffer_size": int(
            os.environ.get("BUFFER_SIZE", str(Config.buffer_size))
        ),
        "resilient": _parse_bool(os.environ.get("RESILIENT", "false")),
    }

    # CLI arg overrides (simple --key=value or --key value parsing)
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith("--"):
            if "=" in arg:
                key, value = arg[2:].split("=", 1)
            elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                key = arg[2:]
                value = argv[i + 1]
                i += 1
            else:
                # Boolean flag with no value (e.g., --compress)
                key = arg[2:]
                value = "true"

            key = key.replace("-", "_")

            if key in ("server_host", "log_file"):
                kwargs[key] = value
            elif key == "server_port":
                kwargs[key] = int(value)
            elif key == "batch_mode":
                kwargs[key] = value.lower() == "batch" or _parse_bool(value)
            elif key == "mode":
                kwargs["batch_mode"] = value.lower() == "batch"
            elif key in ("compress", "resilient"):
                kwargs[key] = _parse_bool(value)
            elif key == "batch_size":
                kwargs[key] = int(value)
            elif key == "metrics_interval":
                kwargs[key] = int(value)
            elif key == "poll_interval":
                kwargs[key] = float(value)
            elif key == "buffer_size":
                kwargs[key] = int(value)
        i += 1

    return Config(**kwargs)
