"""Metadata collector registry and factory.

Provides ``CollectorRegistry`` for managing multiple collectors and
``create_default_registry`` for bootstrapping the standard set.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src.collectors.base import MetadataCollector
from src.collectors.environment import EnvironmentCollector
from src.collectors.performance import PerformanceCollector
from src.collectors.system_info import SystemInfoCollector
from src.config import AppConfig


class CollectorRegistry:
    """Thread-safe registry that holds named ``MetadataCollector`` instances."""

    def __init__(self) -> None:
        self._collectors: Dict[str, MetadataCollector] = {}

    def register(self, collector: MetadataCollector) -> None:
        """Register a collector under its ``.name`` property."""
        self._collectors[collector.name] = collector

    def get(self, name: str) -> Optional[MetadataCollector]:
        """Return the collector registered under *name*, or ``None``."""
        return self._collectors.get(name)

    def collect_from(
        self, names: List[str]
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Run ``collect()`` on each named collector and merge results.

        Returns a ``(merged_metadata, errors)`` tuple.  If a collector
        raises an exception its error message is appended to *errors* and
        processing continues with the next collector.
        """
        merged: Dict[str, Any] = {}
        errors: List[str] = []

        for name in names:
            collector = self.get(name)
            if collector is None:
                errors.append(f"Collector '{name}' not found")
                continue
            try:
                result = collector.collect()
                merged.update(result)
            except Exception as exc:
                errors.append(f"Collector '{name}' failed: {exc}")

        return merged, errors

    def list_collectors(self) -> List[str]:
        """Return the names of all registered collectors."""
        return list(self._collectors.keys())


def create_default_registry(config: AppConfig) -> CollectorRegistry:
    """Create a ``CollectorRegistry`` pre-loaded with the standard collectors."""
    registry = CollectorRegistry()
    registry.register(SystemInfoCollector())
    registry.register(EnvironmentCollector(config))
    registry.register(PerformanceCollector())
    return registry
