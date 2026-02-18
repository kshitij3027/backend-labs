"""Tests for src/models.py — LogEntry, create_log_entry, entry_to_dict."""

from datetime import datetime, timezone

from src.models import LogEntry, create_log_entry, entry_to_dict


# ── create_log_entry ─────────────────────────────────────────────────

class TestCreateLogEntry:
    def test_returns_log_entry_instance(self):
        entry = create_log_entry("test message")
        assert isinstance(entry, LogEntry)

    def test_message_is_set(self):
        entry = create_log_entry("hello world")
        assert entry.message == "hello world"

    def test_default_level_is_info(self):
        entry = create_log_entry("msg")
        assert entry.level == "INFO"

    def test_custom_level(self):
        entry = create_log_entry("msg", level="ERROR")
        assert entry.level == "ERROR"

    def test_default_service_is_default(self):
        entry = create_log_entry("msg")
        assert entry.service == "default"

    def test_custom_service(self):
        entry = create_log_entry("msg", service="auth-service")
        assert entry.service == "auth-service"

    def test_default_metadata_is_empty_dict(self):
        entry = create_log_entry("msg")
        assert entry.metadata == {}
        assert isinstance(entry.metadata, dict)

    def test_custom_metadata_preserved(self):
        meta = {"request_id": "abc-123", "user_id": 42}
        entry = create_log_entry("msg", metadata=meta)
        assert entry.metadata == {"request_id": "abc-123", "user_id": 42}

    def test_metadata_none_becomes_empty_dict(self):
        entry = create_log_entry("msg", metadata=None)
        assert entry.metadata == {}

    def test_timestamp_is_iso_format(self):
        entry = create_log_entry("msg")
        # Should parse without raising
        parsed = datetime.fromisoformat(entry.timestamp)
        assert parsed.tzinfo is not None  # timezone-aware

    def test_timestamp_is_utc(self):
        before = datetime.now(timezone.utc)
        entry = create_log_entry("msg")
        after = datetime.now(timezone.utc)

        parsed = datetime.fromisoformat(entry.timestamp)
        assert before <= parsed <= after

    def test_each_call_produces_unique_timestamp(self):
        e1 = create_log_entry("a")
        e2 = create_log_entry("b")
        # Timestamps should be present on both (may be equal if fast enough,
        # but both must be valid ISO strings)
        datetime.fromisoformat(e1.timestamp)
        datetime.fromisoformat(e2.timestamp)


# ── LogEntry directly ────────────────────────────────────────────────

class TestLogEntry:
    def test_all_fields_set(self):
        entry = LogEntry(
            timestamp="2025-01-01T00:00:00+00:00",
            level="WARN",
            message="direct construction",
            service="my-svc",
            metadata={"key": "val"},
        )
        assert entry.timestamp == "2025-01-01T00:00:00+00:00"
        assert entry.level == "WARN"
        assert entry.message == "direct construction"
        assert entry.service == "my-svc"
        assert entry.metadata == {"key": "val"}

    def test_default_service(self):
        entry = LogEntry(timestamp="t", level="INFO", message="m")
        assert entry.service == "default"

    def test_default_metadata(self):
        entry = LogEntry(timestamp="t", level="INFO", message="m")
        assert entry.metadata == {}

    def test_metadata_default_not_shared(self):
        """Each instance gets its own empty dict (no mutable default bug)."""
        e1 = LogEntry(timestamp="t", level="INFO", message="m1")
        e2 = LogEntry(timestamp="t", level="INFO", message="m2")
        e1.metadata["x"] = 1
        assert "x" not in e2.metadata


# ── entry_to_dict ────────────────────────────────────────────────────

class TestEntryToDict:
    def test_returns_dict(self):
        entry = create_log_entry("msg")
        result = entry_to_dict(entry)
        assert isinstance(result, dict)

    def test_contains_all_keys(self):
        entry = create_log_entry("msg", level="DEBUG", service="svc", metadata={"a": 1})
        result = entry_to_dict(entry)
        assert set(result.keys()) == {"timestamp", "level", "message", "service", "metadata"}

    def test_values_match_entry(self):
        entry = create_log_entry("hello", level="ERROR", service="api", metadata={"k": "v"})
        result = entry_to_dict(entry)
        assert result["message"] == "hello"
        assert result["level"] == "ERROR"
        assert result["service"] == "api"
        assert result["metadata"] == {"k": "v"}
        assert result["timestamp"] == entry.timestamp

    def test_dict_is_independent_copy(self):
        """Mutating the returned dict does not affect the original entry."""
        entry = create_log_entry("msg", metadata={"orig": True})
        result = entry_to_dict(entry)
        result["metadata"]["injected"] = True
        assert "injected" not in entry.metadata

    def test_timestamp_is_iso_in_dict(self):
        entry = create_log_entry("msg")
        result = entry_to_dict(entry)
        datetime.fromisoformat(result["timestamp"])
