"""Tests for the text/syslog log handler."""
from datetime import datetime

import pytest

import src.handlers  # noqa: F401 - triggers handler registration
from src.handlers.text_handler import TextHandler
from src.models import LogLevel


class TestTextHandlerCanHandle:
    """Test can_handle for various text inputs."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_can_handle_rfc5424(self, sample_syslog_rfc5424_bytes):
        assert self.handler.can_handle(sample_syslog_rfc5424_bytes) is True

    def test_can_handle_rfc3164(self, sample_syslog_rfc3164_bytes):
        assert self.handler.can_handle(sample_syslog_rfc3164_bytes) is True

    def test_can_handle_generic_text(self):
        data = b"2024-01-15 10:30:00 INFO Application started"
        assert self.handler.can_handle(data) is True

    def test_can_handle_plain_text(self):
        data = b"hello world"
        assert self.handler.can_handle(data) is True

    def test_can_handle_binary(self):
        data = b"\x00\x01\x02\x03"
        assert self.handler.can_handle(data) is False

    def test_can_handle_empty(self):
        data = b""
        assert self.handler.can_handle(data) is False

    def test_can_handle_json_rejected(self):
        data = b'{"key": "value"}'
        assert self.handler.can_handle(data) is False

    def test_can_handle_json_array_rejected(self):
        data = b'[{"key": "value"}]'
        assert self.handler.can_handle(data) is False


class TestTextHandlerParseRFC5424:
    """Test parsing of RFC 5424 syslog messages."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_parse_rfc5424(self, sample_syslog_rfc5424_bytes):
        entry = self.handler.parse(sample_syslog_rfc5424_bytes)
        assert entry.hostname == "web-01"
        assert entry.service == "api-gateway"
        assert entry.message == "Application started successfully"
        assert entry.timestamp.year == 2024
        assert entry.timestamp.month == 1
        assert entry.timestamp.day == 15
        assert entry.timestamp.hour == 10
        assert entry.timestamp.minute == 30

    def test_parse_rfc5424_severity_mapping(self):
        """Test different priority values map to correct LogLevels."""
        # Priority 165 = facility 20 * 8 + severity 5 (Notice) -> INFO
        data = b"<165>1 2024-01-15T10:30:00Z host app - - - Notice message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.INFO

        # Priority 163 = facility 20 * 8 + severity 3 (Error) -> ERROR
        data = b"<163>1 2024-01-15T10:30:00Z host app - - - Error message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.ERROR

        # Priority 160 = facility 20 * 8 + severity 0 (Emergency) -> CRITICAL
        data = b"<160>1 2024-01-15T10:30:00Z host app - - - Emergency message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.CRITICAL

        # Priority 164 = facility 20 * 8 + severity 4 (Warning) -> WARNING
        data = b"<164>1 2024-01-15T10:30:00Z host app - - - Warning message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.WARNING

        # Priority 167 = facility 20 * 8 + severity 7 (Debug) -> DEBUG
        data = b"<167>1 2024-01-15T10:30:00Z host app - - - Debug message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.DEBUG

    def test_parse_rfc5424_with_procid(self):
        data = b"<165>1 2024-01-15T10:30:00Z host app 12345 - - Message with procid"
        entry = self.handler.parse(data)
        assert entry.metadata["procid"] == "12345"

    def test_parse_rfc5424_with_msgid(self):
        data = b"<165>1 2024-01-15T10:30:00Z host app - MSG001 - Message with msgid"
        entry = self.handler.parse(data)
        assert entry.metadata["msgid"] == "MSG001"


class TestTextHandlerParseRFC3164:
    """Test parsing of RFC 3164 syslog messages."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_parse_rfc3164(self, sample_syslog_rfc3164_bytes):
        entry = self.handler.parse(sample_syslog_rfc3164_bytes)
        assert entry.hostname == "web-01"
        assert entry.service == "sshd"
        assert entry.message == "Connection accepted from 192.168.1.1"
        assert entry.timestamp.month == 1
        assert entry.timestamp.day == 15
        assert entry.timestamp.hour == 10
        assert entry.timestamp.minute == 30

    def test_parse_rfc3164_severity_mapping(self):
        """Test priority -> severity mapping for RFC 3164."""
        # Priority 34 = facility 4 * 8 + severity 2 (Critical) -> CRITICAL
        data = b"<34>Jan 15 10:30:00 myhost myapp[999]: Critical error occurred"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.CRITICAL

        # Priority 38 = facility 4 * 8 + severity 6 (Informational) -> INFO
        data = b"<38>Jan 15 10:30:00 myhost myapp[999]: Info message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.INFO

        # Priority 36 = facility 4 * 8 + severity 4 (Warning) -> WARNING
        data = b"<36>Jan 15 10:30:00 myhost myapp[999]: Warning message"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.WARNING

    def test_parse_rfc3164_with_pid(self):
        data = b"<34>Jan 15 10:30:00 myhost myapp[1234]: Message with pid"
        entry = self.handler.parse(data)
        assert entry.metadata["pid"] == "1234"

    def test_parse_rfc3164_without_pid(self):
        data = b"<34>Jan 15 10:30:00 myhost myapp: Message without pid"
        entry = self.handler.parse(data)
        assert "pid" not in entry.metadata


class TestTextHandlerParseGeneric:
    """Test parsing of generic timestamped text."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_parse_generic_timestamped(self):
        data = b"2024-01-15 10:30:00 ERROR Something failed"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.ERROR
        assert entry.message == "Something failed"
        assert entry.timestamp.year == 2024
        assert entry.timestamp.month == 1
        assert entry.timestamp.day == 15
        assert entry.timestamp.hour == 10
        assert entry.timestamp.minute == 30

    def test_parse_generic_with_iso_timestamp(self):
        data = b"2024-01-15T10:30:00 WARNING Disk space low"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.WARNING
        assert entry.message == "Disk space low"

    def test_parse_generic_with_milliseconds(self):
        data = b"2024-01-15 10:30:00.123 DEBUG Trace event"
        entry = self.handler.parse(data)
        assert entry.level == LogLevel.DEBUG
        assert entry.message == "Trace event"


class TestTextHandlerParseFallback:
    """Test fallback parsing for plain text."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_parse_fallback_plain_text(self):
        data = b"hello world"
        entry = self.handler.parse(data)
        assert entry.message == "hello world"
        assert entry.level == LogLevel.UNKNOWN
        assert isinstance(entry.timestamp, datetime)


class TestTextHandlerMetadata:
    """Test source_format and raw preservation."""

    def setup_method(self):
        self.handler = TextHandler()

    def test_source_format_set(self, sample_syslog_rfc5424_bytes):
        entry = self.handler.parse(sample_syslog_rfc5424_bytes)
        assert entry.source_format == "text"

    def test_raw_preserved(self, sample_syslog_rfc5424_bytes):
        entry = self.handler.parse(sample_syslog_rfc5424_bytes)
        assert entry.raw == sample_syslog_rfc5424_bytes

    def test_source_format_generic(self):
        data = b"2024-01-15 10:30:00 INFO Test message"
        entry = self.handler.parse(data)
        assert entry.source_format == "text"

    def test_raw_preserved_generic(self):
        data = b"2024-01-15 10:30:00 INFO Test message"
        entry = self.handler.parse(data)
        assert entry.raw == data

    def test_source_format_fallback(self):
        data = b"hello world"
        entry = self.handler.parse(data)
        assert entry.source_format == "text"

    def test_raw_preserved_fallback(self):
        data = b"hello world"
        entry = self.handler.parse(data)
        assert entry.raw == data
