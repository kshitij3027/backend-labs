"""Performance metrics metadata collector."""

import time
from typing import Any, Dict, Optional

import psutil

from src.collectors.base import MetadataCollector


class PerformanceCollector(MetadataCollector):
    """Collects live performance metrics (CPU, memory, disk usage).

    Results are cached for ``cache_ttl`` seconds (default 5) to avoid
    hammering the OS for rapidly-successive enrichment requests.
    """

    def __init__(self, cache_ttl: float = 5.0) -> None:
        self._cache_ttl = cache_ttl
        self._cache: Optional[Dict[str, Any]] = None
        self._cache_time: float = 0.0
        # Prime the CPU counter so the first real read is meaningful.
        psutil.cpu_percent(interval=0.1)

    @property
    def name(self) -> str:
        return "performance"

    def collect(self) -> Dict[str, Any]:
        now = time.time()
        if self._cache is not None and (now - self._cache_time) < self._cache_ttl:
            return self._cache

        self._cache = {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage("/").percent,
        }
        self._cache_time = time.time()
        return self._cache
