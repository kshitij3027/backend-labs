import json
import os

import pytest

from src.parsers import detect_format, parse_apache_line, parse_json_line, parse_line


class TestJsonParser:
    def test_valid_json_line(self):
        line = json.dumps(
            {"timestamp": "2025-01-15T00:00:00+00:00", "level": "INFO", "message": "test"}
        )
        result = parse_json_line(line)
        assert result is not None
        assert result["level"] == "INFO"

    def test_malformed_json(self):
        assert parse_json_line("not json at all") is None
        assert parse_json_line("{broken") is None
        assert parse_json_line("") is None

    def test_json_with_all_fields(self, sample_json_logs):
        """First line of generated JSON logs has all expected fields."""
        with open(sample_json_logs) as f:
            line = f.readline()
        result = parse_json_line(line)
        assert result is not None
        for field in [
            "timestamp",
            "level",
            "service",
            "message",
            "ip",
            "url",
            "status_code",
            "user_agent",
        ]:
            assert field in result


class TestApacheParser:
    def test_valid_apache_line(self):
        line = '192.168.1.10 - - [15/Jan/2025:12:00:00 +0000] "GET /api/users HTTP/1.1" 200 1234 "-" "curl/8.4.0"'
        result = parse_apache_line(line)
        assert result is not None
        assert result["ip"] == "192.168.1.10"
        assert result["status_code"] == 200
        assert result["level"] == "INFO"
        assert result["url"] == "/api/users"
        assert result["service"] == "web-server"

    def test_apache_4xx_is_warn(self):
        line = '10.0.0.1 - - [15/Jan/2025:12:00:00 +0000] "GET /missing HTTP/1.1" 404 0 "-" "curl/8.4.0"'
        result = parse_apache_line(line)
        assert result["level"] == "WARN"

    def test_apache_5xx_is_error(self):
        line = '10.0.0.1 - - [15/Jan/2025:12:00:00 +0000] "POST /api/data HTTP/1.1" 500 0 "-" "curl/8.4.0"'
        result = parse_apache_line(line)
        assert result["level"] == "ERROR"

    def test_malformed_apache(self):
        assert parse_apache_line("not an apache line") is None
        assert parse_apache_line("") is None

    def test_apache_from_generated_file(self, sample_apache_logs):
        """All lines in generated Apache file parse correctly."""
        with open(sample_apache_logs) as f:
            for line in f:
                result = parse_apache_line(line)
                assert result is not None, f"Failed to parse: {line}"


class TestDetectFormat:
    def test_detect_json(self, sample_json_logs):
        assert detect_format(sample_json_logs) == "json"

    def test_detect_apache(self, sample_apache_logs):
        assert detect_format(sample_apache_logs) == "apache"

    def test_detect_unknown(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "garbage.log")
        with open(path, "w") as f:
            f.write("this is not a recognized format\n")
        assert detect_format(path) == "unknown"


class TestParseLine:
    def test_dispatch_json(self):
        line = json.dumps({"level": "INFO", "message": "test"})
        result = parse_line(line, "json")
        assert result is not None
        assert result["level"] == "INFO"

    def test_dispatch_apache(self):
        line = '10.0.0.1 - - [15/Jan/2025:12:00:00 +0000] "GET /test HTTP/1.1" 200 100 "-" "curl/8.4.0"'
        result = parse_line(line, "apache")
        assert result is not None

    def test_dispatch_unknown(self):
        assert parse_line("anything", "unknown") is None
