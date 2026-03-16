"""Configuration loader: YAML file + environment variable overrides."""

import os
from typing import Any

import yaml


class Config:
    """Load configuration from a YAML file with environment variable overrides.

    Environment variables take precedence over YAML values so that the same
    image can be reconfigured at deploy time without rebuilding.
    """

    # Maps env-var name -> (yaml path tuple, type converter)
    _ENV_OVERRIDES: list[tuple[str, tuple[str, ...], type]] = [
        ("BOOTSTRAP_SERVERS", ("kafka", "bootstrap_servers"), str),
        ("KAFKA_ACKS", ("kafka", "acks"), str),
        ("KAFKA_BATCH_SIZE", ("kafka", "batch_size"), int),
        ("KAFKA_LINGER_MS", ("kafka", "linger_ms"), int),
        ("KAFKA_COMPRESSION", ("kafka", "compression_type"), str),
        ("PROMETHEUS_PORT", ("prometheus", "port"), int),
        ("DASHBOARD_PORT", ("dashboard", "port"), int),
    ]

    def __init__(self, config_path: str = "config/producer_config.yaml") -> None:
        with open(config_path, "r") as fh:
            self._config: dict[str, Any] = yaml.safe_load(fh)

        self._apply_env_overrides()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_env_overrides(self) -> None:
        """Override YAML values with environment variables when present."""
        for env_var, path, converter in self._ENV_OVERRIDES:
            value = os.environ.get(env_var)
            if value is not None:
                self._set_nested(path, converter(value))

    def _set_nested(self, path: tuple[str, ...], value: Any) -> None:
        """Set a value in a nested dict given a key path."""
        node = self._config
        for key in path[:-1]:
            node = node.setdefault(key, {})
        node[path[-1]] = value

    def _get_nested(self, *path: str) -> Any:
        """Retrieve a value from the nested config dict."""
        node = self._config
        for key in path:
            node = node[key]
        return node

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def bootstrap_servers(self) -> str:
        return self._get_nested("kafka", "bootstrap_servers")

    @property
    def kafka_config(self) -> dict[str, Any]:
        """Return a dict suitable for ``confluent_kafka.Producer(**cfg)``."""
        kafka = self._config["kafka"]
        return {
            "bootstrap.servers": kafka["bootstrap_servers"],
            "acks": str(kafka["acks"]),
            "retries": kafka["retries"],
            "batch.size": kafka["batch_size"],
            "linger.ms": kafka["linger_ms"],
            "compression.type": kafka["compression_type"],
            "enable.idempotence": kafka["enable_idempotence"],
        }

    @property
    def prometheus_port(self) -> int:
        return int(self._get_nested("prometheus", "port"))

    @property
    def dashboard_port(self) -> int:
        return int(self._get_nested("dashboard", "port"))

    @property
    def ws_interval(self) -> int:
        return int(self._get_nested("dashboard", "ws_interval"))

    @property
    def fallback_path(self) -> str:
        return self._get_nested("fallback", "storage_path")
