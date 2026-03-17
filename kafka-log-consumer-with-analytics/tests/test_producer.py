"""Tests for the test log producer."""
import json

import pytest

from src.producer import (
    generate_web_log,
    generate_app_log,
    generate_error_log,
    ENDPOINTS,
    SERVICES,
    ERROR_TYPES,
)


class TestGenerateWebLog:
    def test_structure(self):
        log = generate_web_log()
        assert log["log_type"] == "web_access"
        assert log["endpoint"] in ENDPOINTS
        assert log["method"] in ["GET", "POST", "PUT", "DELETE"]
        assert isinstance(log["status_code"], int)
        assert log["response_time_ms"] > 0
        assert "source_ip" in log
        assert "geo" in log
        assert "timestamp" in log

    def test_json_serializable(self):
        log = generate_web_log()
        json_str = json.dumps(log)
        assert json.loads(json_str) == log


class TestGenerateAppLog:
    def test_structure(self):
        log = generate_app_log()
        assert log["log_type"] == "app_log"
        assert log["service"] in SERVICES
        assert log["component"] in ["handler", "middleware", "database", "cache", "queue"]
        assert log["level"] in ["DEBUG", "INFO", "WARN", "ERROR"]
        assert "message" in log

    def test_json_serializable(self):
        log = generate_app_log()
        assert json.dumps(log)


class TestGenerateErrorLog:
    def test_structure(self):
        log = generate_error_log()
        assert log["log_type"] == "error_log"
        assert log["error_type"] in ERROR_TYPES
        assert "stack_trace" in log
        assert log["endpoint"] in ENDPOINTS
        assert log["severity"] in ["ERROR", "CRITICAL"]
        assert "message" in log

    def test_json_serializable(self):
        log = generate_error_log()
        assert json.dumps(log)


class TestLogDistribution:
    def test_status_code_distribution(self):
        """Verify status codes are within expected range."""
        codes = set()
        for _ in range(1000):
            log = generate_web_log()
            codes.add(log["status_code"])
        # Should see multiple status codes after 1000 samples
        assert len(codes) >= 3

    def test_response_time_positive(self):
        """All response times should be positive."""
        for _ in range(100):
            log = generate_web_log()
            assert log["response_time_ms"] > 0
