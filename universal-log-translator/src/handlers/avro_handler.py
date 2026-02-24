"""Avro OCF log format handler using fastavro."""
from datetime import datetime
from io import BytesIO

import fastavro

from src.base_handler import BaseHandler
from src.models import LogEntry, LogLevel


# Avro Object Container File magic bytes
_AVRO_MAGIC = b"Obj\x01"


class AvroHandler(BaseHandler, format_name="avro"):
    """Handler for Avro Object Container File encoded log entries."""

    def can_handle(self, raw_data: bytes) -> bool:
        """Check if raw_data starts with the Avro OCF magic header.

        Avro Object Container Files always begin with the 4-byte
        magic sequence b'Obj\\x01'.
        """
        return len(raw_data) >= 4 and raw_data[:4] == _AVRO_MAGIC

    def parse(self, raw_data: bytes) -> LogEntry:
        """Parse Avro OCF bytes into a LogEntry.

        Uses fastavro to read the OCF data and extracts the first record.

        Raises:
            ValueError: If the data cannot be parsed as a valid Avro OCF file.
        """
        try:
            reader = fastavro.reader(BytesIO(raw_data))
            record = next(reader)
        except Exception as e:
            raise ValueError(f"Invalid Avro: {e}") from e

        # Parse timestamp string to datetime
        timestamp = self._parse_timestamp(record.get("timestamp", ""))

        # Map level string to LogLevel enum
        level_str = record.get("level", "")
        level = LogLevel.from_string(level_str) if level_str else LogLevel.UNKNOWN

        # Extract message
        message = record.get("message", "")

        # Handle union types: source/hostname/service may be None
        source = record.get("source") or ""
        hostname = record.get("hostname") or ""
        service = record.get("service") or ""

        # Extract metadata map
        metadata = record.get("metadata") or {}

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            source=source,
            hostname=hostname,
            service=service,
            metadata=dict(metadata),
            raw=raw_data,
            source_format="avro",
        )

    @staticmethod
    def _parse_timestamp(ts_str: str) -> datetime:
        """Parse a timestamp string into a datetime, with fallback to now."""
        if not ts_str:
            return datetime.utcnow()

        # Try ISO 8601 first
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue

        return datetime.utcnow()
