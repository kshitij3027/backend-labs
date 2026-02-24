"""Protobuf log format handler using compiled proto definitions."""
from datetime import datetime

from src.base_handler import BaseHandler
from src.models import LogEntry, LogLevel


# Mapping from proto LogLevel enum values to our LogLevel enum
_PROTO_LEVEL_MAP = {
    0: LogLevel.UNKNOWN,    # LOG_LEVEL_UNKNOWN
    1: LogLevel.DEBUG,      # LOG_LEVEL_DEBUG
    2: LogLevel.INFO,       # LOG_LEVEL_INFO
    3: LogLevel.WARNING,    # LOG_LEVEL_WARNING
    4: LogLevel.ERROR,      # LOG_LEVEL_ERROR
    5: LogLevel.CRITICAL,   # LOG_LEVEL_CRITICAL
}

# Avro object container file magic bytes
_AVRO_MAGIC = b"Obj\x01"


class ProtobufHandler(BaseHandler, format_name="protobuf"):
    """Handler for Protocol Buffer encoded log entries."""

    def can_handle(self, raw_data: bytes) -> bool:
        """Check if raw_data looks like a protobuf message using varint heuristics.

        Protobuf messages start with field tags encoded as varints.
        A field tag = (field_number << 3) | wire_type, where wire_type is 0-5.

        This is the most permissive binary check (last resort before 'unknown format').
        """
        if len(raw_data) < 2:
            return False

        # Reject Avro object container files
        if raw_data[:4] == _AVRO_MAGIC:
            return False

        # Reject data that decodes as valid UTF-8 text without control chars
        # (likely syslog, JSON, or plain text -- let those handlers deal with it)
        try:
            text = raw_data.decode("utf-8").strip()
            # If it decodes cleanly as text with no weird control chars, reject it
            control_count = sum(
                1 for c in text if ord(c) < 32 and c not in ("\t", "\n", "\r")
            )
            if control_count == 0 and len(text) > 0:
                return False
        except UnicodeDecodeError:
            # Not valid UTF-8 -- could be protobuf binary
            pass

        # Check first byte for valid protobuf field tag structure
        first_byte = raw_data[0]
        wire_type = first_byte & 0x07
        field_number = first_byte >> 3

        # Valid wire types are 0-5; field_number must be > 0
        if wire_type > 5:
            return False
        if field_number < 1:
            return False

        return True

    def parse(self, raw_data: bytes) -> LogEntry:
        """Parse protobuf bytes into a LogEntry.

        Uses the compiled log_entry_pb2 module to deserialize the data.

        Raises:
            ValueError: If the data cannot be parsed as a valid protobuf LogEntry.
        """
        try:
            from src.generated import log_entry_pb2
        except ImportError as e:
            raise ValueError(
                f"Protobuf generated module not available. Run compile_proto.sh first: {e}"
            ) from e

        try:
            proto_entry = log_entry_pb2.LogEntry()
            proto_entry.ParseFromString(raw_data)
        except Exception as e:
            raise ValueError(f"Invalid Protobuf: {e}") from e

        # Validate that we got at least some meaningful data
        # ParseFromString succeeds on arbitrary bytes, so check for known fields
        if not proto_entry.timestamp and not proto_entry.message:
            raise ValueError(
                "Invalid Protobuf: parsed message has no timestamp or message field"
            )

        # Map proto LogLevel enum to our LogLevel
        level = _PROTO_LEVEL_MAP.get(proto_entry.level, LogLevel.UNKNOWN)

        # Parse timestamp string to datetime
        timestamp = self._parse_timestamp(proto_entry.timestamp)

        # Extract metadata map
        metadata = dict(proto_entry.metadata) if proto_entry.metadata else {}

        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=proto_entry.message,
            source=proto_entry.source,
            hostname=proto_entry.hostname,
            service=proto_entry.service,
            metadata=metadata,
            raw=raw_data,
            source_format="protobuf",
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
