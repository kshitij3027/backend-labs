"""Tests for the Avro OCF log format handler."""
import io
from datetime import datetime

import fastavro
import pytest

from src.handlers.avro_handler import AvroHandler
from src.models import LogLevel


# ── Schema & helper ───────────────────────────────────────────────

AVRO_SCHEMA = {
    "type": "record",
    "name": "LogEntry",
    "namespace": "com.logtranslator",
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "level", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "hostname", "type": ["null", "string"], "default": None},
        {"name": "service", "type": ["null", "string"], "default": None},
        {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}},
    ],
}
PARSED_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA)


def make_avro_bytes(record: dict) -> bytes:
    """Serialize a single record to Avro OCF bytes."""
    buf = io.BytesIO()
    fastavro.writer(buf, PARSED_SCHEMA, [record])
    return buf.getvalue()


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def handler():
    return AvroHandler()


@pytest.fixture
def full_record_bytes():
    return make_avro_bytes({
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Application started successfully",
        "source": "app-server",
        "hostname": "web-01",
        "service": "api-gateway",
        "metadata": {},
    })


# ── can_handle tests ─────────────────────────────────────────────

class TestCanHandle:

    def test_can_handle_avro(self, handler, full_record_bytes):
        """Avro OCF bytes should be recognized."""
        assert handler.can_handle(full_record_bytes) is True

    def test_can_handle_json_rejected(self, handler):
        """JSON bytes should be rejected."""
        assert handler.can_handle(b'{"message": "hello"}') is False

    def test_can_handle_text_rejected(self, handler):
        """Plain text should be rejected."""
        assert handler.can_handle(b"2024-01-15 10:30:00 INFO hello") is False

    def test_can_handle_empty(self, handler):
        """Empty bytes should be rejected."""
        assert handler.can_handle(b"") is False

    def test_can_handle_non_avro_binary(self, handler):
        """Random binary data without the Avro magic should be rejected."""
        assert handler.can_handle(b"\x00\x01\x02\x03\x04\x05") is False


# ── parse tests ───────────────────────────────────────────────────

class TestParse:

    def test_parse_avro(self, handler, full_record_bytes):
        """Full record should be parsed into correct LogEntry fields."""
        entry = handler.parse(full_record_bytes)
        assert entry.timestamp == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.level == LogLevel.INFO
        assert entry.message == "Application started successfully"
        assert entry.source == "app-server"
        assert entry.hostname == "web-01"
        assert entry.service == "api-gateway"

    def test_parse_avro_with_metadata(self, handler):
        """Record with metadata should preserve the metadata dict."""
        data = make_avro_bytes({
            "timestamp": "2024-01-15T10:30:00",
            "level": "ERROR",
            "message": "Disk full",
            "source": "monitor",
            "hostname": "storage-01",
            "service": "disk-checker",
            "metadata": {"disk": "/dev/sda1", "usage": "99%"},
        })
        entry = handler.parse(data)
        assert entry.metadata == {"disk": "/dev/sda1", "usage": "99%"}

    def test_parse_avro_nullable_fields(self, handler):
        """Record with null source/hostname/service should default to empty strings."""
        data = make_avro_bytes({
            "timestamp": "2024-01-15T10:30:00",
            "level": "WARNING",
            "message": "Nullable test",
            "source": None,
            "hostname": None,
            "service": None,
            "metadata": {},
        })
        entry = handler.parse(data)
        assert entry.source == ""
        assert entry.hostname == ""
        assert entry.service == ""

    def test_parse_avro_level_mapping(self, handler):
        """Different level strings should map to correct LogLevel values."""
        level_pairs = [
            ("DEBUG", LogLevel.DEBUG),
            ("INFO", LogLevel.INFO),
            ("WARNING", LogLevel.WARNING),
            ("WARN", LogLevel.WARNING),
            ("ERROR", LogLevel.ERROR),
            ("CRITICAL", LogLevel.CRITICAL),
            ("FATAL", LogLevel.CRITICAL),
            ("UNKNOWN", LogLevel.UNKNOWN),
        ]
        for level_str, expected_level in level_pairs:
            data = make_avro_bytes({
                "timestamp": "2024-01-15T10:30:00",
                "level": level_str,
                "message": f"Level test: {level_str}",
                "source": None,
                "hostname": None,
                "service": None,
                "metadata": {},
            })
            entry = handler.parse(data)
            assert entry.level == expected_level, (
                f"Level '{level_str}' should map to {expected_level}, got {entry.level}"
            )

    def test_parse_malformed_avro(self, handler):
        """Magic bytes followed by garbage should raise ValueError."""
        malformed = b"Obj\x01" + b"\xff\xfe\xfd\xfc\xfb\xfa"
        with pytest.raises(ValueError, match="Invalid Avro"):
            handler.parse(malformed)

    def test_source_format_set(self, handler, full_record_bytes):
        """source_format should be set to 'avro'."""
        entry = handler.parse(full_record_bytes)
        assert entry.source_format == "avro"

    def test_raw_preserved(self, handler, full_record_bytes):
        """raw should be the original bytes."""
        entry = handler.parse(full_record_bytes)
        assert entry.raw == full_record_bytes
