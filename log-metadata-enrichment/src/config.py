"""Application configuration with environment variable overrides and YAML rule loading."""

from __future__ import annotations

from dataclasses import dataclass, field

import yaml
from dotenv import dotenv_values


@dataclass
class AppConfig:
    """Application configuration with sensible defaults."""

    service_name: str = "log-enrichment"
    environment: str = "development"
    version: str = "1.0.0"
    region: str = "local"
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    rules_path: str = "config/enrichment_rules.yaml"


def load_config() -> AppConfig:
    """Load configuration from environment variables with .env file support.

    Environment variables (if set) override the AppConfig defaults:
        SERVICE_NAME, ENVIRONMENT, VERSION, REGION,
        HOST, PORT, DEBUG, RULES_PATH
    """
    env = dotenv_values(".env")

    config = AppConfig()

    mapping = {
        "SERVICE_NAME": "service_name",
        "ENVIRONMENT": "environment",
        "VERSION": "version",
        "REGION": "region",
        "HOST": "host",
        "PORT": "port",
        "DEBUG": "debug",
        "RULES_PATH": "rules_path",
    }

    for env_var, attr in mapping.items():
        value = env.get(env_var)
        if value is not None:
            if attr == "port":
                setattr(config, attr, int(value))
            elif attr == "debug":
                setattr(config, attr, value.lower() in ("true", "1", "yes"))
            else:
                setattr(config, attr, value)

    return config


def load_rules(path: str) -> dict:
    """Load enrichment rules from a YAML file.

    Returns an empty dict on any error (file not found, parse error, etc.).
    """
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
