"""Shared test fixtures."""
import pytest


@pytest.fixture
def syslog_rfc3164_line():
    return "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8"


@pytest.fixture
def syslog_rfc5424_line():
    return '<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry'


@pytest.fixture
def journald_line():
    return "Feb 14 06:36:01 myhost systemd[1]: Started Session 123 of User root."


@pytest.fixture
def json_log_line():
    return '{"timestamp": "2024-01-15T10:30:00Z", "level": "ERROR", "message": "Connection timeout", "hostname": "web-01"}'


@pytest.fixture
def sample_lines(syslog_rfc3164_line, syslog_rfc5424_line, journald_line, json_log_line):
    return [syslog_rfc3164_line, syslog_rfc5424_line, journald_line, json_log_line]
