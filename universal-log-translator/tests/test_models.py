"""Tests for core data models."""
import pytest
from datetime import datetime

from src.models import LogEntry, LogLevel, UnsupportedFormatError
from src.base_handler import BaseHandler
from src.detector import FormatDetector


class TestLogLevel:
    """Tests for LogLevel enum."""

    def test_all_levels_exist(self):
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "UNKNOWN"]
        for level in levels:
            assert hasattr(LogLevel, level)

    def test_from_string_exact(self):
        assert LogLevel.from_string("INFO") == LogLevel.INFO
        assert LogLevel.from_string("ERROR") == LogLevel.ERROR

    def test_from_string_case_insensitive(self):
        assert LogLevel.from_string("info") == LogLevel.INFO
        assert LogLevel.from_string("Error") == LogLevel.ERROR

    def test_from_string_aliases(self):
        assert LogLevel.from_string("WARN") == LogLevel.WARNING
        assert LogLevel.from_string("FATAL") == LogLevel.CRITICAL
        assert LogLevel.from_string("CRIT") == LogLevel.CRITICAL
        assert LogLevel.from_string("ERR") == LogLevel.ERROR

    def test_from_string_unknown(self):
        assert LogLevel.from_string("BOGUS") == LogLevel.UNKNOWN
        assert LogLevel.from_string("") == LogLevel.UNKNOWN

    def test_from_string_whitespace(self):
        assert LogLevel.from_string("  INFO  ") == LogLevel.INFO


class TestLogEntry:
    """Tests for LogEntry dataclass."""

    def test_creation(self, sample_log_entry):
        assert sample_log_entry.message == "Test log message"
        assert sample_log_entry.level == LogLevel.INFO
        assert sample_log_entry.source == "test-source"

    def test_defaults(self):
        entry = LogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            message="test",
        )
        assert entry.source == ""
        assert entry.hostname == ""
        assert entry.service == ""
        assert entry.metadata == {}
        assert entry.raw == b""
        assert entry.source_format == ""

    def test_to_dict(self, sample_log_entry):
        d = sample_log_entry.to_dict()
        assert d["level"] == "INFO"
        assert d["message"] == "Test log message"
        assert d["source"] == "test-source"
        assert d["hostname"] == "test-host"
        assert d["service"] == "test-service"
        assert d["metadata"] == {"key": "value"}
        assert d["source_format"] == "test"
        assert "timestamp" in d

    def test_to_dict_iso_timestamp(self, sample_log_entry):
        d = sample_log_entry.to_dict()
        # Should be ISO format parseable
        datetime.fromisoformat(d["timestamp"])


class TestUnsupportedFormatError:
    """Tests for UnsupportedFormatError."""

    def test_is_exception(self):
        assert issubclass(UnsupportedFormatError, Exception)

    def test_message(self):
        err = UnsupportedFormatError("test error")
        assert str(err) == "test error"


class TestBaseHandlerRegistry:
    """Tests for BaseHandler auto-registration."""

    def test_registry_exists(self):
        registry = BaseHandler.get_registry()
        assert isinstance(registry, dict)

    def test_subclass_registration(self):
        """Test that defining a subclass with format_name registers it."""
        # Create a test handler
        class _TestHandler(BaseHandler, format_name="_test_internal"):
            def can_handle(self, raw_data: bytes) -> bool:
                return False
            def parse(self, raw_data: bytes) -> LogEntry:
                raise NotImplementedError

        registry = BaseHandler.get_registry()
        assert "_test_internal" in registry
        assert registry["_test_internal"] is _TestHandler

        # Cleanup
        del BaseHandler._registry["_test_internal"]

    def test_get_handler(self):
        class _TestHandler2(BaseHandler, format_name="_test_internal2"):
            def can_handle(self, raw_data: bytes) -> bool:
                return False
            def parse(self, raw_data: bytes) -> LogEntry:
                raise NotImplementedError

        handler = BaseHandler.get_handler("_test_internal2")
        assert isinstance(handler, _TestHandler2)

        # Cleanup
        del BaseHandler._registry["_test_internal2"]

    def test_get_handler_unknown(self):
        with pytest.raises(KeyError, match="No handler registered"):
            BaseHandler.get_handler("nonexistent")


class TestFormatDetector:
    """Tests for FormatDetector."""

    def test_no_handlers_raises(self):
        """With no registered handlers matching, detect should raise."""
        detector = FormatDetector(handler_order=[])
        with pytest.raises(UnsupportedFormatError):
            detector.detect(b"some data")

    def test_detect_with_matching_handler(self):
        class _TestDetectHandler(BaseHandler, format_name="_test_detect"):
            def can_handle(self, raw_data: bytes) -> bool:
                return raw_data == b"match_me"
            def parse(self, raw_data: bytes) -> LogEntry:
                raise NotImplementedError

        detector = FormatDetector(handler_order=["_test_detect"])
        handler = detector.detect(b"match_me")
        assert handler.format_name == "_test_detect"

        # Cleanup
        del BaseHandler._registry["_test_detect"]

    def test_detect_no_match_raises(self):
        class _TestNoMatch(BaseHandler, format_name="_test_nomatch"):
            def can_handle(self, raw_data: bytes) -> bool:
                return False
            def parse(self, raw_data: bytes) -> LogEntry:
                raise NotImplementedError

        detector = FormatDetector(handler_order=["_test_nomatch"])
        with pytest.raises(UnsupportedFormatError):
            detector.detect(b"no match")

        # Cleanup
        del BaseHandler._registry["_test_nomatch"]
