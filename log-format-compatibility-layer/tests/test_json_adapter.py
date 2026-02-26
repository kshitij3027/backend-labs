"""Tests for the JSON log adapter."""
import pytest
import json
from src.adapters.json_adapter import JsonLogAdapter
from src.models import SeverityLevel


class TestJsonAdapter:
    def setup_method(self):
        self.adapter = JsonLogAdapter()

    def test_format_name(self):
        assert self.adapter.format_name == "json"

    def test_can_handle_valid_json(self, json_log_line):
        confidence = self.adapter.can_handle(json_log_line)
        assert confidence == 0.95

    def test_can_handle_invalid_json(self):
        assert self.adapter.can_handle("not json at all") == 0.0
        assert self.adapter.can_handle("<34>Oct 11 syslog line") == 0.0

    def test_parse_standard_json(self, json_log_line):
        parsed = self.adapter.parse(json_log_line)
        assert parsed.source_format == "json"
        assert parsed.message == "Connection timeout"
        assert parsed.level == SeverityLevel.ERROR
        assert parsed.hostname == "web-01"
        assert parsed.timestamp is not None
        assert parsed.raw == json_log_line

    def test_parse_alternative_keys(self):
        """Test parsing with alternative key names (ts, severity, msg, host)."""
        line = '{"ts": "2024-01-15T10:30:15.000Z", "severity": "DEBUG", "msg": "Cache hit", "host": "cache-01"}'
        parsed = self.adapter.parse(line)
        assert parsed.message == "Cache hit"
        assert parsed.level == SeverityLevel.DEBUG
        assert parsed.hostname == "cache-01"
        assert parsed.timestamp is not None
