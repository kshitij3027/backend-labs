"""Tests for the Protobuf log format handler."""
import json
from datetime import datetime

import pytest

from src.generated import log_entry_pb2
from src.handlers.protobuf_handler import ProtobufHandler
from src.models import LogLevel


class TestProtobufCanHandle:
    """Tests for ProtobufHandler.can_handle()."""

    def test_can_handle_protobuf(self, sample_protobuf_bytes):
        """Serialized protobuf bytes should be detected."""
        handler = ProtobufHandler()
        assert handler.can_handle(sample_protobuf_bytes) is True

    def test_can_handle_json_rejected(self):
        """JSON bytes should not be detected as protobuf."""
        handler = ProtobufHandler()
        data = json.dumps({"message": "hello", "level": "INFO"}).encode("utf-8")
        assert handler.can_handle(data) is False

    def test_can_handle_text_rejected(self):
        """Plain text should not be detected as protobuf."""
        handler = ProtobufHandler()
        data = b"2024-01-15 10:30:00 INFO Application started successfully"
        assert handler.can_handle(data) is False

    def test_can_handle_empty(self):
        """Empty bytes should not be detected as protobuf."""
        handler = ProtobufHandler()
        assert handler.can_handle(b"") is False

    def test_can_handle_avro_rejected(self):
        """Avro object container magic bytes should not be detected as protobuf."""
        handler = ProtobufHandler()
        # Avro magic: Obj\x01 followed by some data
        avro_data = b"Obj\x01\x00\x00\x00\x00some avro content here"
        assert handler.can_handle(avro_data) is False


class TestProtobufParse:
    """Tests for ProtobufHandler.parse()."""

    def test_parse_protobuf(self, sample_protobuf_bytes):
        """Parsing serialized protobuf should yield correct fields."""
        handler = ProtobufHandler()
        entry = handler.parse(sample_protobuf_bytes)

        assert entry.timestamp == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.level == LogLevel.INFO
        assert entry.message == "Application started successfully"
        assert entry.source == "app-server"
        assert entry.hostname == "web-01"
        assert entry.service == "api-gateway"

    def test_parse_protobuf_level_mapping(self):
        """All proto log levels should map to correct LogLevel enum values."""
        handler = ProtobufHandler()

        level_mapping = {
            log_entry_pb2.LOG_LEVEL_UNKNOWN: LogLevel.UNKNOWN,
            log_entry_pb2.LOG_LEVEL_DEBUG: LogLevel.DEBUG,
            log_entry_pb2.LOG_LEVEL_INFO: LogLevel.INFO,
            log_entry_pb2.LOG_LEVEL_WARNING: LogLevel.WARNING,
            log_entry_pb2.LOG_LEVEL_ERROR: LogLevel.ERROR,
            log_entry_pb2.LOG_LEVEL_CRITICAL: LogLevel.CRITICAL,
        }

        for proto_level, expected_level in level_mapping.items():
            proto_entry = log_entry_pb2.LogEntry()
            proto_entry.timestamp = "2024-01-15T10:30:00"
            proto_entry.level = proto_level
            proto_entry.message = "Test message"
            raw = proto_entry.SerializeToString()

            entry = handler.parse(raw)
            assert entry.level == expected_level, (
                f"Proto level {proto_level} should map to {expected_level}, "
                f"got {entry.level}"
            )

    def test_parse_protobuf_with_metadata(self):
        """Proto with metadata map should parse metadata dict correctly."""
        handler = ProtobufHandler()

        proto_entry = log_entry_pb2.LogEntry()
        proto_entry.timestamp = "2024-01-15T10:30:00"
        proto_entry.level = log_entry_pb2.LOG_LEVEL_INFO
        proto_entry.message = "Request handled"
        proto_entry.metadata["request_id"] = "abc-123"
        proto_entry.metadata["trace_id"] = "trace-456"
        proto_entry.metadata["user_agent"] = "Mozilla/5.0"
        raw = proto_entry.SerializeToString()

        entry = handler.parse(raw)
        assert entry.metadata == {
            "request_id": "abc-123",
            "trace_id": "trace-456",
            "user_agent": "Mozilla/5.0",
        }

    def test_parse_malformed_protobuf(self):
        """Random binary that passes can_handle but fails parse should raise ValueError."""
        handler = ProtobufHandler()
        # Craft bytes that look like a valid protobuf tag but contain garbage
        # Field 1, wire type 2 (length-delimited), then invalid length
        malformed = bytes([0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F])
        with pytest.raises(ValueError, match="Invalid Protobuf"):
            handler.parse(malformed)

    def test_source_format_set(self, sample_protobuf_bytes):
        """source_format should be 'protobuf'."""
        handler = ProtobufHandler()
        entry = handler.parse(sample_protobuf_bytes)
        assert entry.source_format == "protobuf"

    def test_raw_preserved(self, sample_protobuf_bytes):
        """raw field should be the original bytes."""
        handler = ProtobufHandler()
        entry = handler.parse(sample_protobuf_bytes)
        assert entry.raw == sample_protobuf_bytes
