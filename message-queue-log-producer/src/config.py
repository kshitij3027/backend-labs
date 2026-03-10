"""Configuration loader for the message-queue-log-producer."""

import os
import yaml


class Config:
    """Loads configuration from config.yaml with env var overrides."""

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.environ.get("CONFIG_PATH", "config.yaml")

        with open(config_path, "r") as f:
            self._data = yaml.safe_load(f)

        self._apply_env_overrides()

    def _apply_env_overrides(self):
        """Override config values with environment variables when set."""
        env_map = {
            "RABBITMQ_HOST": ("rabbitmq", "host", str),
            "RABBITMQ_PORT": ("rabbitmq", "port", int),
            "RABBITMQ_USER": ("rabbitmq", "user", str),
            "RABBITMQ_PASS": ("rabbitmq", "password", str),
            "BATCH_SIZE": ("batch", "max_size", int),
            "BATCH_FLUSH_INTERVAL": ("batch", "flush_interval", float),
            "CIRCUIT_BREAKER_THRESHOLD": ("circuit_breaker", "failure_threshold", int),
            "CIRCUIT_BREAKER_TIMEOUT": ("circuit_breaker", "recovery_timeout", int),
            "HTTP_PORT": (None, "http_port", int),
            "QUEUE_MAXSIZE": (None, "queue_maxsize", int),
        }

        for env_var, (section, key, cast) in env_map.items():
            value = os.environ.get(env_var)
            if value is not None:
                if section is not None:
                    self._data[section][key] = cast(value)
                else:
                    self._data[key] = cast(value)

    @property
    def rabbitmq(self):
        return self._data["rabbitmq"]

    @property
    def exchange(self):
        return self._data["exchange"]

    @property
    def queue(self):
        return self._data["queue"]

    @property
    def dead_letter(self):
        return self._data["dead_letter"]

    @property
    def batch(self):
        return self._data["batch"]

    @property
    def circuit_breaker(self):
        return self._data["circuit_breaker"]

    @property
    def queue_maxsize(self):
        return self._data["queue_maxsize"]

    @property
    def http_port(self):
        return self._data["http_port"]
