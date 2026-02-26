"""Tests for the format detection engine."""
import pytest
from src.detection import FormatDetectionEngine


class TestFormatDetectionEngine:
    def setup_method(self):
        self.engine = FormatDetectionEngine()

    def test_detect_json(self, json_log_line):
        result = self.engine.detect_line(json_log_line)
        assert result is not None
        fmt, confidence = result
        assert fmt == "json"
        assert confidence >= 0.9

    def test_detect_syslog_rfc3164(self, syslog_rfc3164_line):
        result = self.engine.detect_line(syslog_rfc3164_line)
        assert result is not None
        fmt, confidence = result
        assert fmt == "syslog_rfc3164"
        assert confidence >= 0.85

    def test_detect_syslog_rfc5424(self, syslog_rfc5424_line):
        result = self.engine.detect_line(syslog_rfc5424_line)
        assert result is not None
        fmt, confidence = result
        assert fmt == "syslog_rfc5424"
        assert confidence >= 0.9

    def test_detect_journald(self, journald_line):
        result = self.engine.detect_line(journald_line)
        assert result is not None
        fmt, confidence = result
        assert fmt == "journald"
        assert confidence >= 0.5

    def test_detect_batch(self, sample_lines):
        result = self.engine.detect_batch(sample_lines)
        assert result["total_lines"] == 4
        assert result["detected_lines"] == 4
        assert result["detection_rate"] == 1.0
        assert len(result["formats"]) >= 3  # at least json, syslog, journald

    def test_parse_line(self, json_log_line):
        parsed = self.engine.parse_line(json_log_line)
        assert parsed is not None
        assert parsed.source_format == "json"
        assert parsed.message == "Connection timeout"

    def test_detect_unrecognized(self):
        result = self.engine.detect_line("this is just plain text with no format")
        # May or may not be detected by journald depending on heuristics
        # If detected, confidence should be low
        if result is not None:
            _, confidence = result
            assert confidence < 0.5
