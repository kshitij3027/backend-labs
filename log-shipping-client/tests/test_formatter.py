"""Tests for formatter module."""

import json
from src.formatter import parse_log_line, format_ndjson


class TestParseLogLine:
    def test_info_line(self):
        result = parse_log_line("2024-01-15 08:23:45 INFO Application started successfully")
        assert result == {
            "timestamp": "2024-01-15 08:23:45",
            "level": "INFO",
            "message": "Application started successfully",
        }

    def test_warning_line(self):
        result = parse_log_line("2024-01-15 08:24:01 WARNING High memory usage detected: 85%")
        assert result["level"] == "WARNING"
        assert "85%" in result["message"]

    def test_error_line(self):
        result = parse_log_line("2024-01-15 08:24:15 ERROR Failed to connect to database: timeout after 30s")
        assert result["level"] == "ERROR"
        assert "timeout" in result["message"]

    def test_debug_line(self):
        result = parse_log_line("2024-01-15 08:25:30 DEBUG Cache hit ratio: 0.92 for session abc-def")
        assert result["level"] == "DEBUG"

    def test_critical_line(self):
        result = parse_log_line("2024-01-15 08:30:00 CRITICAL System shutdown imminent")
        assert result["level"] == "CRITICAL"

    def test_level_normalized_to_upper(self):
        result = parse_log_line("2024-01-15 08:23:45 info lowercase level")
        assert result["level"] == "INFO"

    def test_empty_line(self):
        assert parse_log_line("") is None

    def test_whitespace_only(self):
        assert parse_log_line("   \t  ") is None

    def test_missing_message(self):
        assert parse_log_line("2024-01-15 08:23:45 INFO") is None

    def test_missing_level(self):
        assert parse_log_line("2024-01-15 08:23:45") is None

    def test_garbage_line(self):
        assert parse_log_line("not a valid log line at all") is None

    def test_special_characters_in_message(self):
        result = parse_log_line('2024-01-15 08:23:45 INFO user="john" action="login" ip=192.168.1.1')
        assert result is not None
        assert 'user="john"' in result["message"]

    def test_message_with_colons(self):
        result = parse_log_line("2024-01-15 08:23:45 ERROR Error: connection: refused")
        assert result["message"] == "Error: connection: refused"


class TestFormatNdjson:
    def test_basic_serialization(self):
        entry = {"level": "INFO", "message": "test"}
        result = format_ndjson(entry)
        assert result.endswith(b"\n")
        parsed = json.loads(result.decode("utf-8"))
        assert parsed == entry

    def test_compact_json(self):
        entry = {"level": "INFO", "message": "test"}
        result = format_ndjson(entry)
        # No spaces after separators
        assert b": " not in result
        assert b", " not in result

    def test_utf8_encoding(self):
        entry = {"level": "INFO", "message": "caf\u00e9 \u2603"}
        result = format_ndjson(entry)
        assert isinstance(result, bytes)
        parsed = json.loads(result.decode("utf-8"))
        assert parsed["message"] == "caf\u00e9 \u2603"

    def test_roundtrip(self):
        line = "2024-01-15 08:23:45 INFO Application started"
        entry = parse_log_line(line)
        ndjson_bytes = format_ndjson(entry)
        roundtripped = json.loads(ndjson_bytes.decode("utf-8"))
        assert roundtripped == entry
