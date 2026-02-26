"""Adapter registry for log format detection and parsing."""
from typing import List, Optional, Tuple
from src.adapters.base import LogFormatAdapter
from src.models import ParsedLog
from src.config import HIGH_CONFIDENCE_THRESHOLD


class AdapterRegistry:
    """Registry of log format adapters with detection and parsing."""

    def __init__(self):
        self._adapters: List[LogFormatAdapter] = []

    def register(self, adapter: LogFormatAdapter) -> None:
        """Register an adapter with the registry."""
        self._adapters.append(adapter)

    @property
    def adapters(self) -> List[LogFormatAdapter]:
        """Return list of registered adapters."""
        return list(self._adapters)

    def detect(self, line: str) -> Optional[Tuple[LogFormatAdapter, float]]:
        """
        Detect the format of a log line.

        Returns (adapter, confidence) tuple for the best match,
        or None if no adapter can handle the line.
        Short-circuits if confidence exceeds HIGH_CONFIDENCE_THRESHOLD.
        """
        best_adapter = None
        best_confidence = 0.0

        for adapter in self._adapters:
            confidence = adapter.can_handle(line)
            if confidence > best_confidence:
                best_adapter = adapter
                best_confidence = confidence
                if best_confidence > HIGH_CONFIDENCE_THRESHOLD:
                    break

        if best_adapter is None or best_confidence <= 0.0:
            return None

        return (best_adapter, best_confidence)

    def detect_and_parse(self, line: str) -> Optional[ParsedLog]:
        """
        Detect the format and parse the log line.

        Returns a ParsedLog with confidence set, or None if unrecognized.
        """
        result = self.detect(line)
        if result is None:
            return None

        adapter, confidence = result
        parsed = adapter.parse(line)
        parsed.confidence = confidence
        return parsed


# Global registry instance
_registry = AdapterRegistry()


def get_registry() -> AdapterRegistry:
    """Get the global adapter registry."""
    return _registry


def register_adapter(adapter: LogFormatAdapter) -> None:
    """Register an adapter with the global registry."""
    _registry.register(adapter)
