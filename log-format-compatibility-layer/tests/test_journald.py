"""Tests for the Journald adapter."""
import pytest
from src.adapters.journald import JournaldAdapter
from src.models import SeverityLevel


class TestJournaldAdapter:
    def setup_method(self):
        self.adapter = JournaldAdapter()

    def test_format_name(self):
        assert self.adapter.format_name == "journald"

    def test_can_handle_journald_systemd(self, journald_line):
        confidence = self.adapter.can_handle(journald_line)
        assert confidence >= 0.7  # systemd[1] + timestamp + lifecycle verb

    def test_can_handle_rejects_syslog(self, syslog_rfc3164_line):
        """Must return 0.0 for syslog lines (they start with <PRI>)."""
        confidence = self.adapter.can_handle(syslog_rfc3164_line)
        assert confidence == 0.0

    def test_can_handle_rejects_json(self, json_log_line):
        confidence = self.adapter.can_handle(json_log_line)
        assert confidence == 0.0

    def test_parse_systemd_line(self, journald_line):
        parsed = self.adapter.parse(journald_line)
        assert parsed.source_format == "journald"
        assert parsed.hostname == "myhost"
        assert parsed.app_name == "systemd"
        assert parsed.pid == 1
        assert "Started Session" in parsed.message
        assert parsed.timestamp is not None

    def test_severity_inference(self):
        """Test that severity is inferred from message keywords."""
        error_line = "Feb 14 06:36:01 myhost app[123]: Connection error: timeout"
        parsed = self.adapter.parse(error_line)
        assert parsed.level == SeverityLevel.ERROR

        warn_line = "Feb 14 06:36:01 myhost app[123]: Warning: disk space low"
        parsed = self.adapter.parse(warn_line)
        assert parsed.level == SeverityLevel.WARNING

        normal_line = "Feb 14 06:36:01 myhost app[123]: Service started successfully"
        parsed = self.adapter.parse(normal_line)
        assert parsed.level == SeverityLevel.INFORMATIONAL
