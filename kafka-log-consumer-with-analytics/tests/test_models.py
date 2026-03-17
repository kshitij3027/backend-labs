"""Tests for log message models."""
import json
import time

import pytest

from src.models import (
    AppLog,
    ErrorLog,
    WebAccessLog,
    parse_log_message,
    log_to_json,
)


class TestWebAccessLog:
    def test_defaults(self):
        log = WebAccessLog()
        assert log.log_type == "web_access"
        assert log.method == "GET"
        assert log.status_code == 200

    def test_from_dict(self, sample_web_log):
        log = WebAccessLog(**sample_web_log)
        assert log.endpoint == "/api/users"
        assert log.response_time_ms == 45.2
        assert log.geo == "us-east"

    def test_round_trip(self, sample_web_log):
        log = WebAccessLog(**sample_web_log)
        json_str = log_to_json(log)
        data = json.loads(json_str)
        restored = WebAccessLog(**data)
        assert restored.endpoint == log.endpoint
        assert restored.status_code == log.status_code
        assert restored.response_time_ms == log.response_time_ms


class TestAppLog:
    def test_defaults(self):
        log = AppLog()
        assert log.log_type == "app_log"
        assert log.level == "INFO"

    def test_from_dict(self, sample_app_log):
        log = AppLog(**sample_app_log)
        assert log.service == "auth-service"
        assert log.message == "User logged in successfully"


class TestErrorLog:
    def test_defaults(self):
        log = ErrorLog()
        assert log.log_type == "error_log"
        assert log.severity == "ERROR"

    def test_from_dict(self, sample_error_log):
        log = ErrorLog(**sample_error_log)
        assert log.error_type == "NullPointerException"
        assert log.endpoint == "/api/orders"


class TestParseLogMessage:
    def test_parse_web_log(self, sample_web_log):
        value = json.dumps(sample_web_log).encode()
        result = parse_log_message(value, topic="web-logs")
        assert isinstance(result, WebAccessLog)
        assert result.endpoint == "/api/users"
        assert result.topic == "web-logs"

    def test_parse_app_log(self, sample_app_log):
        value = json.dumps(sample_app_log).encode()
        result = parse_log_message(value, topic="app-logs")
        assert isinstance(result, AppLog)
        assert result.service == "auth-service"

    def test_parse_error_log(self, sample_error_log):
        value = json.dumps(sample_error_log).encode()
        result = parse_log_message(value, topic="error-logs")
        assert isinstance(result, ErrorLog)
        assert result.error_type == "NullPointerException"

    def test_parse_by_log_type(self, sample_web_log):
        """Parsing without topic should use log_type field."""
        value = json.dumps(sample_web_log).encode()
        result = parse_log_message(value)
        assert isinstance(result, WebAccessLog)

    def test_parse_invalid_json(self):
        result = parse_log_message(b"not json")
        assert result is None

    def test_parse_invalid_bytes(self):
        result = parse_log_message(b"\xff\xfe")
        assert result is None

    def test_parse_fallback_web(self):
        """Unknown log_type but has status_code -> WebAccessLog."""
        data = {"status_code": 200, "response_time_ms": 10.0}
        result = parse_log_message(json.dumps(data).encode())
        assert isinstance(result, WebAccessLog)

    def test_parse_fallback_error(self):
        """Unknown log_type but has stack_trace -> ErrorLog."""
        data = {"stack_trace": "error here", "severity": "CRITICAL"}
        result = parse_log_message(json.dumps(data).encode())
        assert isinstance(result, ErrorLog)

    def test_parse_fallback_app(self):
        """Unknown log_type, no distinguishing fields -> AppLog."""
        data = {"message": "hello"}
        result = parse_log_message(json.dumps(data).encode())
        assert isinstance(result, AppLog)
