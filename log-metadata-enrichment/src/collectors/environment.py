"""Environment / service-context metadata collector."""

from typing import Any, Dict, Optional

from src.collectors.base import MetadataCollector
from src.config import AppConfig


class EnvironmentCollector(MetadataCollector):
    """Collects service-context metadata from the application configuration.

    Results are cached forever after the first call since the config is
    immutable during the lifetime of the process.
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._cache: Optional[Dict[str, Any]] = None

    @property
    def name(self) -> str:
        return "environment"

    def collect(self) -> Dict[str, Any]:
        if self._cache is not None:
            return self._cache

        self._cache = {
            "service_name": self._config.service_name,
            "environment": self._config.environment,
            "version": self._config.version,
            "region": self._config.region,
        }
        return self._cache
