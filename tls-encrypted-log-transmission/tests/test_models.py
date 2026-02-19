"""Tests for log entry models."""

from src.models import create_log_entry


class TestCreateLogEntry:
    def test_basic_entry(self):
        entry = create_log_entry("INFO", "test message")
        assert entry["level"] == "INFO"
        assert entry["message"] == "test message"
        assert "timestamp" in entry

    def test_level_uppercased(self):
        entry = create_log_entry("warning", "low disk")
        assert entry["level"] == "WARNING"

    def test_extra_fields(self):
        entry = create_log_entry("ERROR", "fail", source="app", code=500)
        assert entry["source"] == "app"
        assert entry["code"] == 500

    def test_timestamp_is_iso_format(self):
        entry = create_log_entry("DEBUG", "check")
        # ISO format contains 'T' separator
        assert "T" in entry["timestamp"]
