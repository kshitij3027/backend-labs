"""LogNormalizer: wires format detection with parsing."""
from src.base_handler import BaseHandler
from src.detector import FormatDetector
from src.models import LogEntry, UnsupportedFormatError


class LogNormalizer:
    """Main entry point for log normalization.

    Auto-detects format and parses raw bytes into LogEntry.
    Can also accept explicit format hints.
    """
    # Default detection order: most distinctive first
    DEFAULT_ORDER = ["avro", "json", "text", "protobuf"]

    def __init__(self, handler_order: list[str] | None = None):
        order = handler_order or self.DEFAULT_ORDER
        self._detector = FormatDetector(handler_order=order)

    def normalize(self, raw_data: bytes, source_format: str | None = None) -> LogEntry:
        """Normalize raw log bytes into a LogEntry.

        Args:
            raw_data: Raw log bytes in any supported format.
            source_format: Optional format hint (e.g., "json", "text", "protobuf", "avro").
                          If provided, skips auto-detection and uses the specified handler.

        Returns:
            Parsed LogEntry.

        Raises:
            UnsupportedFormatError: If no handler can parse the data.
            ValueError: If the data is malformed for the detected/specified format.
        """
        if source_format:
            handler = BaseHandler.get_handler(source_format)
            return handler.parse(raw_data)

        handler = self._detector.detect(raw_data)
        return handler.parse(raw_data)

    @property
    def registered_formats(self) -> list[str]:
        """List all registered format names."""
        return list(BaseHandler.get_registry().keys())
