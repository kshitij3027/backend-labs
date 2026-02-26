"""Tests for the Syslog RFC 5424 adapter."""
import pytest
from src.adapters.syslog_rfc5424 import SyslogRFC5424Adapter
from src.models import SeverityLevel


class TestSyslogRFC5424Adapter:
    def setup_method(self):
        self.adapter = SyslogRFC5424Adapter()

    def test_format_name(self):
        assert self.adapter.format_name == "syslog_rfc5424"

    def test_can_handle_valid_rfc5424(self, syslog_rfc5424_line):
        confidence = self.adapter.can_handle(syslog_rfc5424_line)
        assert confidence == 0.95

    def test_can_handle_non_rfc5424(self):
        assert self.adapter.can_handle("just a plain message") == 0.0
        assert self.adapter.can_handle('<34>Oct 11 22:14:15 mymachine su: test') == 0.0  # RFC 3164, not 5424

    def test_parse_standard_line(self, syslog_rfc5424_line):
        parsed = self.adapter.parse(syslog_rfc5424_line)
        assert parsed.source_format == "syslog_rfc5424"
        assert parsed.priority == 165
        assert parsed.hostname == "mymachine.example.com"
        assert parsed.app_name == "evntslog"
        assert parsed.level == SeverityLevel.NOTICE  # 165 & 0x07 = 5 -> NOTICE
        assert parsed.facility == "local4"  # 165 >> 3 = 20 -> local4
        assert "application event log entry" in parsed.message.lower()
        assert parsed.timestamp is not None
        # Check structured data was parsed into metadata
        assert "exampleSDID@32473" in str(parsed.metadata) or len(parsed.metadata) > 0
