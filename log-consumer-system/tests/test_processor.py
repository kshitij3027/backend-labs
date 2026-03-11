"""Tests for src.processor — log parsing and message processing."""

from __future__ import annotations

import pytest

from src.processor import LogProcessor


@pytest.fixture
def processor():
    return LogProcessor()


class TestParseLogLine:
    """Tests for LogProcessor.parse_log_line."""

    def test_parse_valid_log_line(self, processor: LogProcessor):
        """Standard combined format without response time."""
        raw = (
            '192.168.1.1 - - [10/Mar/2026:13:55:36 +0000] '
            '"GET /api/users HTTP/1.1" 200 1234 "-" "curl/7.68"'
        )
        entry = processor.parse_log_line(raw)

        assert entry is not None
        assert entry.ip == "192.168.1.1"
        assert entry.method == "GET"
        assert entry.path == "/api/users"
        assert entry.status_code == 200
        assert entry.response_size == 1234
        assert entry.response_time_ms is None
        assert entry.timestamp is not None
        assert entry.timestamp.year == 2026
        assert entry.timestamp.month == 3
        assert entry.timestamp.day == 10
        assert entry.raw == raw

    def test_parse_log_line_with_response_time(self, processor: LogProcessor):
        """Combined format with trailing response time in milliseconds."""
        raw = (
            '10.0.0.5 - - [10/Mar/2026:14:00:00 +0000] '
            '"POST /api/orders HTTP/1.1" 201 512 "https://example.com" "Mozilla/5.0" 78.3'
        )
        entry = processor.parse_log_line(raw)

        assert entry is not None
        assert entry.ip == "10.0.0.5"
        assert entry.method == "POST"
        assert entry.path == "/api/orders"
        assert entry.status_code == 201
        assert entry.response_size == 512
        assert entry.response_time_ms == pytest.approx(78.3)
        assert entry.timestamp is not None

    def test_parse_malformed_line(self, processor: LogProcessor):
        """A line that doesn't match combined format returns None."""
        assert processor.parse_log_line("this is not a log line") is None

    def test_parse_empty_line(self, processor: LogProcessor):
        """Empty / whitespace-only lines return None."""
        assert processor.parse_log_line("") is None
        assert processor.parse_log_line("   ") is None
        assert processor.parse_log_line("\n") is None

    def test_parse_minimal_combined_format(self, processor: LogProcessor):
        """Minimal combined format without referrer/UA or response time."""
        raw = '127.0.0.1 - - [01/Jan/2025:00:00:00 +0000] "DELETE /items/42 HTTP/1.1" 204 0'
        entry = processor.parse_log_line(raw)

        assert entry is not None
        assert entry.ip == "127.0.0.1"
        assert entry.method == "DELETE"
        assert entry.path == "/items/42"
        assert entry.status_code == 204
        assert entry.response_size == 0
        assert entry.response_time_ms is None


class TestProcessMessage:
    """Tests for LogProcessor.process_message."""

    def test_process_message_with_dict(self, processor: LogProcessor):
        """Dict with a 'log' key is parsed correctly."""
        msg = {
            "log": (
                '10.0.0.1 - - [10/Mar/2026:12:00:00 +0000] '
                '"GET /health HTTP/1.1" 200 2 "-" "python-requests/2.31"'
            )
        }
        entry = processor.process_message(msg)
        assert entry is not None
        assert entry.path == "/health"
        assert entry.status_code == 200

    def test_process_message_missing_log_key(self, processor: LogProcessor):
        """Dict without 'log' key returns None."""
        assert processor.process_message({"data": "something"}) is None
        assert processor.process_message({}) is None
