"""Tests for core data models."""

import uuid

from src.models import LogMessage, Priority


class TestPriority:
    def test_priority_ordering(self):
        assert Priority.CRITICAL < Priority.HIGH < Priority.MEDIUM < Priority.LOW

    def test_priority_values(self):
        assert Priority.CRITICAL == 0
        assert Priority.HIGH == 1
        assert Priority.MEDIUM == 2
        assert Priority.LOW == 3


class TestLogMessage:
    def test_log_message_creation(self):
        msg = LogMessage(
            priority=Priority.HIGH,
            source="test-service",
            message="something happened",
        )
        assert msg.priority == Priority.HIGH
        assert msg.source == "test-service"
        assert msg.message == "something happened"
        assert isinstance(msg.timestamp, float)
        assert isinstance(msg.created_at, float)

    def test_log_message_default_id(self):
        msg = LogMessage()
        # Should be a valid UUID-4 string
        parsed = uuid.UUID(msg.id, version=4)
        assert str(parsed) == msg.id

    def test_log_message_original_priority(self):
        msg = LogMessage(priority=Priority.MEDIUM)
        assert msg.original_priority == Priority.MEDIUM

    def test_log_message_to_dict(self):
        msg = LogMessage(
            id="abc-123",
            timestamp=1000.0,
            created_at=1000.0,
            priority=Priority.CRITICAL,
            source="svc",
            message="boom",
        )
        d = msg.to_dict()
        assert d["id"] == "abc-123"
        assert d["timestamp"] == 1000.0
        assert d["created_at"] == 1000.0
        assert d["priority"] == "CRITICAL"
        assert d["source"] == "svc"
        assert d["message"] == "boom"
        assert d["original_priority"] == "CRITICAL"

    def test_log_message_from_dict(self):
        data = {
            "id": "xyz-789",
            "timestamp": 2000.0,
            "created_at": 2000.0,
            "priority": "HIGH",
            "source": "api",
            "message": "slow",
            "original_priority": "MEDIUM",
        }
        msg = LogMessage.from_dict(data)
        assert msg.id == "xyz-789"
        assert msg.priority == Priority.HIGH
        assert msg.original_priority == Priority.MEDIUM
        assert msg.source == "api"

    def test_log_message_round_trip(self):
        original = LogMessage(
            priority=Priority.HIGH,
            source="round-trip",
            message="test payload",
        )
        restored = LogMessage.from_dict(original.to_dict())

        assert restored.id == original.id
        assert restored.timestamp == original.timestamp
        assert restored.created_at == original.created_at
        assert restored.priority == original.priority
        assert restored.source == original.source
        assert restored.message == original.message
        assert restored.original_priority == original.original_priority
