"""System information metadata collector."""

import platform
import socket
from typing import Any, Dict, Optional

from src.collectors.base import MetadataCollector


class SystemInfoCollector(MetadataCollector):
    """Collects static system information (hostname, OS, Python version).

    Results are cached forever after the first call since system info
    does not change during process lifetime.
    """

    def __init__(self) -> None:
        self._cache: Optional[Dict[str, Any]] = None

    @property
    def name(self) -> str:
        return "system_info"

    def collect(self) -> Dict[str, Any]:
        if self._cache is not None:
            return self._cache

        self._cache = {
            "hostname": socket.gethostname(),
            "os_info": f"{platform.system()} {platform.release()}",
            "python_version": platform.python_version(),
        }
        return self._cache
