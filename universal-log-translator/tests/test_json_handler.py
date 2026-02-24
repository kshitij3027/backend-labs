"""Tests for the JSON log handler."""
import json
from datetime import datetime, timedelta

import pytest

import src.handlers  # noqa: F401 - triggers handler registration
from src.handlers.json_handler import JsonHandler
from src.models import LogLevel


class TestCanHandle:
    """Tests for JsonHandler.can_handle()."""

    def test_can_handle_json_object(self):
        handler = JsonHandler()
        assert handler.can_handle(b'{"key": "value"}') is True

    def test_can_handle_json_array(self):
        handler = JsonHandler()
        assert handler.can_handle(b'[{"key": "value"}]') is True

    def test_can_handle_with_whitespace(self):
        handler = JsonHandler()
        assert handler.can_handle(b'  {"key": "value"}') is True

    def test_can_handle_plain_text(self):
        handler = JsonHandler()
        assert handler.can_handle(b"hello world") is False

    def test_can_handle_empty(self):
        handler = JsonHandler()
        assert handler.can_handle(b"") is False


class TestParse:
    """Tests for JsonHandler.parse()."""

    def test_parse_standard_fields(self, sample_json_bytes):
        handler = JsonHandler()
        entry = handler.parse(sample_json_bytes)

        assert entry.timestamp == datetime(2024, 1, 15, 10, 30, 0)
        assert entry.level == LogLevel.INFO
        assert entry.message == "Application started successfully"
        assert entry.source == "app-server"
        assert entry.hostname == "web-01"
        assert entry.service == "api-gateway"

    def test_parse_flexible_timestamp_keys(self):
        handler = JsonHandler()
        aliases = ["ts", "time", "@timestamp"]
        for alias in aliases:
            data = {alias: "2024-06-01T12:00:00", "message": "test"}
            raw = json.dumps(data).encode("utf-8")
            entry = handler.parse(raw)
            assert entry.timestamp == datetime(2024, 6, 1, 12, 0, 0), (
                f"Failed for timestamp alias: {alias}"
            )

    def test_parse_flexible_level_keys(self):
        handler = JsonHandler()
        aliases = ["severity", "log_level", "lvl"]
        for alias in aliases:
            data = {alias: "ERROR", "message": "test"}
            raw = json.dumps(data).encode("utf-8")
            entry = handler.parse(raw)
            assert entry.level == LogLevel.ERROR, (
                f"Failed for level alias: {alias}"
            )

    def test_parse_flexible_message_keys(self):
        handler = JsonHandler()
        aliases = ["msg", "text", "log"]
        for alias in aliases:
            data = {alias: "hello from alias"}
            raw = json.dumps(data).encode("utf-8")
            entry = handler.parse(raw)
            assert entry.message == "hello from alias", (
                f"Failed for message alias: {alias}"
            )

    def test_parse_hostname_aliases(self):
        handler = JsonHandler()
        data = {"host": "server-42", "message": "test"}
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.hostname == "server-42"

    def test_parse_service_aliases(self):
        handler = JsonHandler()
        for alias in ["service_name", "app"]:
            data = {alias: "my-service", "message": "test"}
            raw = json.dumps(data).encode("utf-8")
            entry = handler.parse(raw)
            assert entry.service == "my-service", (
                f"Failed for service alias: {alias}"
            )

    def test_parse_extra_fields_to_metadata(self):
        handler = JsonHandler()
        data = {
            "message": "test",
            "request_id": "abc-123",
            "user_id": 42,
            "tags": ["web", "prod"],
        }
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.metadata == {
            "request_id": "abc-123",
            "user_id": 42,
            "tags": ["web", "prod"],
        }

    def test_parse_missing_timestamp_defaults(self):
        handler = JsonHandler()
        data = {"message": "no timestamp here"}
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        # Should default to roughly "now"
        assert abs((entry.timestamp - datetime.utcnow()).total_seconds()) < 5

    def test_parse_missing_level_defaults(self):
        handler = JsonHandler()
        data = {"message": "no level here"}
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.level == LogLevel.INFO

    def test_parse_malformed_json_raises(self, sample_malformed_json_bytes):
        handler = JsonHandler()
        with pytest.raises(ValueError, match="Invalid JSON"):
            handler.parse(sample_malformed_json_bytes)

    def test_parse_json_array_first_element(self):
        handler = JsonHandler()
        data = [
            {"message": "first", "level": "WARNING"},
            {"message": "second", "level": "ERROR"},
        ]
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.message == "first"
        assert entry.level == LogLevel.WARNING

    def test_source_format_set(self):
        handler = JsonHandler()
        data = {"message": "test"}
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.source_format == "json"

    def test_raw_preserved(self):
        handler = JsonHandler()
        data = {"message": "test"}
        raw = json.dumps(data).encode("utf-8")
        entry = handler.parse(raw)
        assert entry.raw == raw
