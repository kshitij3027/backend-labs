"""Abstract base class for metadata collectors."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class MetadataCollector(ABC):
    """Base class that all metadata collectors must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this collector."""
        ...

    @abstractmethod
    def collect(self) -> Dict[str, Any]:
        """Collect and return metadata as a dictionary."""
        ...
