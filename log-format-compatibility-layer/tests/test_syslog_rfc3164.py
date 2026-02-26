"""Tests for the Syslog RFC 3164 adapter."""
import pytest
from src.adapters.syslog_rfc3164 import SyslogRFC3164Adapter
from src.models import SeverityLevel


class TestSyslogRFC3164Adapter:
    def setup_method(self):
        self.adapter = SyslogRFC3164Adapter()

    def test_format_name(self):
        assert self.adapter.format_name == "syslog_rfc3164"

    def test_can_handle_valid_syslog(self, syslog_rfc3164_line):
        confidence = self.adapter.can_handle(syslog_rfc3164_line)
        assert confidence == 0.90

    def test_can_handle_non_syslog(self):
        assert self.adapter.can_handle("just a plain message") == 0.0
        assert self.adapter.can_handle('{"json": "log"}') == 0.0

    def test_parse_standard_line(self, syslog_rfc3164_line):
        parsed = self.adapter.parse(syslog_rfc3164_line)
        assert parsed.source_format == "syslog_rfc3164"
        assert parsed.priority == 34
        assert parsed.hostname == "mymachine"
        assert parsed.app_name == "su"
        assert parsed.level == SeverityLevel.CRITICAL  # 34 & 0x07 = 2 -> CRITICAL
        assert parsed.facility == "auth"  # 34 >> 3 = 4 -> auth
        assert "su root" in parsed.message
        assert parsed.raw == syslog_rfc3164_line

    def test_parse_with_pid(self):
        line = '<38>Jan  1 00:00:00 router sshd[4567]: Accepted publickey for admin'
        parsed = self.adapter.parse(line)
        assert parsed.app_name == "sshd"
        assert parsed.pid == 4567
        assert parsed.hostname == "router"
