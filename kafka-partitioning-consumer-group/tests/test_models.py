"""Tests for log entry models."""
import json
import pytest
from src.models import LogEntry, LogLevel


class TestLogLevel:
    def test_enum_values(self):
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"


class TestLogEntry:
    def test_creation_with_defaults(self):
        entry = LogEntry()
        assert entry.id
        assert entry.timestamp > 0
        assert entry.level == LogLevel.INFO
        assert entry.request_id

    def test_creation_with_values(self):
        entry = LogEntry(
            level=LogLevel.ERROR,
            service="auth-service",
            message="Auth failed",
            user_id="5678",
        )
        assert entry.level == LogLevel.ERROR
        assert entry.service == "auth-service"
        assert entry.user_id == "5678"

    def test_serialization_roundtrip(self):
        original = LogEntry(
            level=LogLevel.WARNING,
            service="api-gateway",
            message="Slow response",
            user_id="1234",
            metadata={"latency_ms": 500},
        )
        serialized = original.to_kafka_value()
        assert isinstance(serialized, bytes)

        restored = LogEntry.from_kafka_value(serialized)
        assert restored.id == original.id
        assert restored.level == original.level
        assert restored.service == original.service
        assert restored.user_id == original.user_id
        assert restored.metadata == original.metadata

    def test_partition_key_with_user_id(self):
        entry = LogEntry(user_id="1234")
        assert entry.partition_key() == "1234"

    def test_partition_key_without_user_id(self):
        entry = LogEntry(user_id="")
        assert entry.partition_key() is None

    def test_to_kafka_value_is_valid_json(self):
        entry = LogEntry(service="test", message="hello")
        data = json.loads(entry.to_kafka_value())
        assert "service" in data
        assert "message" in data
        assert "level" in data
