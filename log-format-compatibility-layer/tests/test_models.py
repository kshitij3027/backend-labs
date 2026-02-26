"""Tests for the core data models."""
from datetime import datetime, timezone

from src.models import ParsedLog, SeverityLevel


class TestSeverityLevel:
    """Tests for the SeverityLevel enum."""

    def test_severity_level_values(self):
        """Verify all 8 severity levels have correct integer values."""
        assert SeverityLevel.EMERGENCY == 0
        assert SeverityLevel.ALERT == 1
        assert SeverityLevel.CRITICAL == 2
        assert SeverityLevel.ERROR == 3
        assert SeverityLevel.WARNING == 4
        assert SeverityLevel.NOTICE == 5
        assert SeverityLevel.INFORMATIONAL == 6
        assert SeverityLevel.DEBUG == 7

    def test_severity_from_syslog(self):
        """from_syslog_severity(3) should return ERROR."""
        assert SeverityLevel.from_syslog_severity(3) == SeverityLevel.ERROR

    def test_severity_from_syslog_invalid(self):
        """Out-of-range severity should return DEBUG."""
        assert SeverityLevel.from_syslog_severity(99) == SeverityLevel.DEBUG
        assert SeverityLevel.from_syslog_severity(-1) == SeverityLevel.DEBUG

    def test_severity_from_string(self):
        """Test common string aliases map to correct levels."""
        assert SeverityLevel.from_string("warn") == SeverityLevel.WARNING
        assert SeverityLevel.from_string("err") == SeverityLevel.ERROR
        assert SeverityLevel.from_string("info") == SeverityLevel.INFORMATIONAL
        assert SeverityLevel.from_string("fatal") == SeverityLevel.EMERGENCY

    def test_severity_from_string_case_insensitive(self):
        """String parsing should be case-insensitive."""
        assert SeverityLevel.from_string("Error") == SeverityLevel.ERROR
        assert SeverityLevel.from_string("WARNING") == SeverityLevel.WARNING
        assert SeverityLevel.from_string("Debug") == SeverityLevel.DEBUG


class TestParsedLog:
    """Tests for the ParsedLog dataclass."""

    def test_parsed_log_defaults(self):
        """Default ParsedLog should have empty message and 0.0 confidence."""
        log = ParsedLog()
        assert log.message == ""
        assert log.confidence == 0.0
        assert log.timestamp is None
        assert log.level is None
        assert log.source_format == ""
        assert log.facility is None
        assert log.hostname is None
        assert log.priority is None
        assert log.app_name is None
        assert log.pid is None
        assert log.metadata == {}
        assert log.raw == ""

    def test_parsed_log_to_dict(self):
        """to_dict() should serialize all fields correctly."""
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        log = ParsedLog(
            timestamp=ts,
            level=SeverityLevel.ERROR,
            message="Connection timeout",
            source_format="json",
            hostname="web-01",
            confidence=0.95,
            raw='{"level": "ERROR", "message": "Connection timeout"}',
        )
        result = log.to_dict()

        assert result["timestamp"] == "2024-01-15T10:30:00+00:00"
        assert result["level"] == "ERROR"
        assert result["message"] == "Connection timeout"
        assert result["source_format"] == "json"
        assert result["hostname"] == "web-01"
        assert result["confidence"] == 0.95
        assert result["raw"] == '{"level": "ERROR", "message": "Connection timeout"}'
        assert result["facility"] is None
        assert result["priority"] is None
        assert result["app_name"] is None
        assert result["pid"] is None
        assert result["metadata"] == {}
