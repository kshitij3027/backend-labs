"""Shared test fixtures for universal-log-translator."""
import pytest
from datetime import datetime

from src.models import LogEntry, LogLevel


@pytest.fixture
def sample_log_entry():
    """A basic LogEntry for testing."""
    return LogEntry(
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        level=LogLevel.INFO,
        message="Test log message",
        source="test-source",
        hostname="test-host",
        service="test-service",
        metadata={"key": "value"},
        raw=b"raw test data",
        source_format="test",
    )


@pytest.fixture
def sample_json_bytes():
    """Sample JSON log entry as bytes."""
    import json
    data = {
        "timestamp": "2024-01-15T10:30:00",
        "level": "INFO",
        "message": "Application started successfully",
        "source": "app-server",
        "hostname": "web-01",
        "service": "api-gateway",
    }
    return json.dumps(data).encode("utf-8")


@pytest.fixture
def sample_syslog_rfc5424_bytes():
    """Sample RFC 5424 syslog message."""
    return b"<165>1 2024-01-15T10:30:00.000Z web-01 api-gateway 1234 - - Application started successfully"


@pytest.fixture
def sample_syslog_rfc3164_bytes():
    """Sample RFC 3164 syslog message."""
    return b"<34>Jan 15 10:30:00 web-01 sshd[1234]: Connection accepted from 192.168.1.1"


@pytest.fixture
def sample_text_bytes():
    """Sample plain text log with timestamp."""
    return b"2024-01-15 10:30:00 INFO Application started successfully"
