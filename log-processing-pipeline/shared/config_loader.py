"""Loads the central YAML config file used by all pipeline components."""

import os
import yaml


def load_yaml(path: str = "/app/config.yml") -> dict:
    """Load YAML config from *path* and return as a dict.

    The path can be overridden via the ``CONFIG_PATH`` environment variable.
    """
    path = os.environ.get("CONFIG_PATH", path)
    with open(path, "r") as f:
        return yaml.safe_load(f)
