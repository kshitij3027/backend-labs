"""Tests for output formatters."""
import json
import pytest
from datetime import datetime, timezone
from src.formatters import format_json, format_structured, format_plain, get_formatter
from src.models import ParsedLog, SeverityLevel


class TestFormatters:
    def setup_method(self):
        self.log = ParsedLog(
            timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
            level=SeverityLevel.ERROR,
            message="Connection timeout",
            source_format="json",
            hostname="web-01",
            app_name="nginx",
            pid=1234,
        )

    def test_format_json(self):
        output = format_json(self.log)
        data = json.loads(output)
        assert data["message"] == "Connection timeout"
        assert data["level"] == "ERROR"
        assert data["hostname"] == "web-01"

    def test_format_structured(self):
        output = format_structured(self.log)
        assert "level=ERROR" in output
        assert "hostname=web-01" in output
        assert "msg=Connection timeout" in output
        assert " | " in output

    def test_format_plain(self):
        output = format_plain(self.log)
        assert "[ERROR]" in output
        assert "web-01" in output
        assert "Connection timeout" in output

    def test_get_formatter(self):
        assert get_formatter("json") == format_json
        assert get_formatter("structured") == format_structured
        assert get_formatter("plain") == format_plain
        # Unknown format defaults to json
        assert get_formatter("unknown") == format_json
