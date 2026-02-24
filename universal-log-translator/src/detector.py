"""Format detection via ordered probe over registered handlers."""
from src.base_handler import BaseHandler
from src.models import UnsupportedFormatError


class FormatDetector:
    """Detects log format by probing handlers in order."""

    def __init__(self, handler_order: list[str] | None = None):
        """Initialize with optional handler probe order.

        Args:
            handler_order: List of format names in probe order.
                          If None, uses all registered handlers.
        """
        self._handler_order = handler_order

    @property
    def handlers(self) -> list[BaseHandler]:
        """Get handler instances in probe order."""
        registry = BaseHandler.get_registry()
        if self._handler_order:
            return [registry[name]() for name in self._handler_order if name in registry]
        return [cls() for cls in registry.values()]

    def detect(self, raw_data: bytes) -> BaseHandler:
        """Detect the format of raw log data.

        Returns the first handler whose can_handle() returns True.
        Raises UnsupportedFormatError if no handler matches.
        """
        for handler in self.handlers:
            if handler.can_handle(raw_data):
                return handler
        raise UnsupportedFormatError(
            f"No handler can parse the given data ({len(raw_data)} bytes)"
        )
