"""Tests for the log entry formatter."""

from src.formatter import format_log_entry


class TestFormatLogEntry:
    def test_basic_structure(self):
        entry = format_log_entry(1, "INFO", "hello", app="test-app", host="testhost")
        assert entry["sequence"] == 1
        assert entry["level"] == "INFO"
        assert entry["message"] == "hello"
        assert entry["app"] == "test-app"
        assert entry["host"] == "testhost"
        assert "timestamp" in entry

    def test_level_uppercased(self):
        entry = format_log_entry(1, "error", "fail")
        assert entry["level"] == "ERROR"

    def test_timestamp_format(self):
        entry = format_log_entry(1, "INFO", "test")
        ts = entry["timestamp"]
        assert ts.endswith("Z")
        assert "T" in ts

    def test_default_host(self):
        entry = format_log_entry(1, "INFO", "test")
        assert len(entry["host"]) > 0
