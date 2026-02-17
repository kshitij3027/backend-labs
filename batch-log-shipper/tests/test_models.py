"""Tests for the log entry model."""

import datetime

from src.models import LogEntry, create_log_entry, entry_to_dict


def test_create_log_entry_defaults():
    entry = create_log_entry(level="INFO", message="test message")
    assert entry.level == "INFO"
    assert entry.message == "test message"
    assert entry.service == "batch-log-shipper"
    assert entry.timestamp is not None
    assert len(entry.timestamp) > 0


def test_create_log_entry_custom():
    meta = {"request_id": "abc-123"}
    entry = create_log_entry(
        level="ERROR",
        message="something broke",
        service="my-service",
        metadata=meta,
    )
    assert entry.level == "ERROR"
    assert entry.message == "something broke"
    assert entry.service == "my-service"
    assert entry.metadata == {"request_id": "abc-123"}


def test_entry_to_dict():
    entry = create_log_entry(level="WARNING", message="disk full")
    d = entry_to_dict(entry)
    assert isinstance(d, dict)
    assert "timestamp" in d
    assert "level" in d
    assert "message" in d
    assert "service" in d
    assert "metadata" in d
    assert d["level"] == "WARNING"
    assert d["message"] == "disk full"


def test_auto_timestamp():
    entry = LogEntry()
    ts = datetime.datetime.fromisoformat(entry.timestamp)
    now = datetime.datetime.now(datetime.timezone.utc)
    # Timestamp should be within the last 2 seconds
    delta = (now - ts).total_seconds()
    assert 0 <= delta < 2


def test_default_metadata_is_empty_dict():
    entry = LogEntry()
    assert entry.metadata == {}
    assert isinstance(entry.metadata, dict)
