"""Configuration loader for smart log routing pipeline."""

import os

import yaml


class Config:
    """Loads and provides access to routing configuration from YAML and env vars."""

    def __init__(self, config_path="config/routing_config.yaml"):
        with open(config_path, "r") as f:
            self._config = yaml.safe_load(f)

        # Allow environment variable overrides
        rabbitmq_host = os.environ.get("RABBITMQ_HOST")
        if rabbitmq_host:
            self._config["rabbitmq"]["host"] = rabbitmq_host

        rabbitmq_port = os.environ.get("RABBITMQ_PORT")
        if rabbitmq_port:
            self._config["rabbitmq"]["port"] = int(rabbitmq_port)

        rabbitmq_user = os.environ.get("RABBITMQ_USER")
        if rabbitmq_user:
            self._config["rabbitmq"]["credentials"]["username"] = rabbitmq_user

        rabbitmq_pass = os.environ.get("RABBITMQ_PASS")
        if rabbitmq_pass:
            self._config["rabbitmq"]["credentials"]["password"] = rabbitmq_pass

    @property
    def host(self):
        return self._config["rabbitmq"]["host"]

    @property
    def port(self):
        return self._config["rabbitmq"]["port"]

    @property
    def management_port(self):
        return self._config["rabbitmq"]["management_port"]

    @property
    def username(self):
        return self._config["rabbitmq"]["credentials"]["username"]

    @property
    def password(self):
        return self._config["rabbitmq"]["credentials"]["password"]

    @property
    def heartbeat(self):
        return self._config["rabbitmq"]["heartbeat"]

    @property
    def blocked_connection_timeout(self):
        return self._config["rabbitmq"]["blocked_connection_timeout"]

    @property
    def retry_max(self):
        return self._config["rabbitmq"]["connection"]["retry_max"]

    @property
    def retry_delay(self):
        return self._config["rabbitmq"]["connection"]["retry_delay"]

    def get_connection_params(self):
        """Return a dict of connection parameters for pika."""
        return {
            "host": self.host,
            "port": self.port,
            "credentials": {
                "username": self.username,
                "password": self.password,
            },
            "heartbeat": self.heartbeat,
            "blocked_connection_timeout": self.blocked_connection_timeout,
        }

    def get_exchange_configs(self):
        """Return the list of exchange configuration dicts."""
        return [dict(e) for e in self._config["exchanges"]]

    def get_queue_configs(self):
        """Return the list of queue configuration dicts."""
        return [dict(q) for q in self._config["queues"]]
