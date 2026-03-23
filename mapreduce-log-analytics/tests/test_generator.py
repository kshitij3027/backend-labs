"""Tests for the log data generator."""

import json
import os
import re

import pytest

from src.generator import generate_apache_logs, generate_json_logs

REQUIRED_JSON_FIELDS = {
    "timestamp",
    "level",
    "service",
    "message",
    "ip",
    "url",
    "status_code",
    "user_agent",
    "user_id",
}

# Apache combined log format regex
APACHE_PATTERN = re.compile(
    r'^(\S+) \S+ \S+ \[([^\]]+)\] '
    r'"(\S+) (\S+) (\S+)" (\d{3}) (\d+) '
    r'"([^"]*)" "([^"]*)"$'
)


@pytest.mark.unit
class TestJsonGenerator:
    def test_produces_valid_lines(self, sample_json_logs):
        """Each line must be valid JSON with all required fields."""
        with open(sample_json_logs) as f:
            lines = f.readlines()

        assert len(lines) == 100

        for line in lines:
            record = json.loads(line.strip())
            assert REQUIRED_JSON_FIELDS.issubset(record.keys()), (
                f"Missing fields: {REQUIRED_JSON_FIELDS - record.keys()}"
            )

    def test_deterministic(self, tmp_output_dir):
        """Same seed produces identical output."""
        path1 = os.path.join(tmp_output_dir, "det1.jsonl")
        path2 = os.path.join(tmp_output_dir, "det2.jsonl")

        generate_json_logs(path1, num_lines=200, seed=42)
        generate_json_logs(path2, num_lines=200, seed=42)

        with open(path1) as f1, open(path2) as f2:
            assert f1.read() == f2.read()

    def test_level_distribution(self, tmp_output_dir):
        """Level counts should approximate expected weights over 10K lines."""
        path = os.path.join(tmp_output_dir, "dist.jsonl")
        stats = generate_json_logs(path, num_lines=10_000, seed=42)

        counts = stats["level_counts"]
        total = stats["total_lines"]

        # INFO should be ~60% (allow 50-70%)
        assert 0.50 <= counts.get("INFO", 0) / total <= 0.70
        # WARN should be ~20% (allow 14-26%)
        assert 0.14 <= counts.get("WARN", 0) / total <= 0.26
        # ERROR should be ~15% (allow 10-20%)
        assert 0.10 <= counts.get("ERROR", 0) / total <= 0.20
        # FATAL should be ~5% (allow 2-8%)
        assert 0.02 <= counts.get("FATAL", 0) / total <= 0.08

    def test_includes_404_status(self, sample_json_logs):
        """At least some lines should have status_code 404."""
        with open(sample_json_logs) as f:
            records = [json.loads(line) for line in f]

        status_codes = {r["status_code"] for r in records}
        assert 404 in status_codes

    def test_includes_varied_ips(self, sample_json_logs):
        """Should have at least 5 unique IPs."""
        with open(sample_json_logs) as f:
            records = [json.loads(line) for line in f]

        unique_ips = {r["ip"] for r in records}
        assert len(unique_ips) >= 5

    def test_includes_user_agents(self, sample_json_logs):
        """Should have at least 3 unique user agents."""
        with open(sample_json_logs) as f:
            records = [json.loads(line) for line in f]

        unique_agents = {r["user_agent"] for r in records}
        assert len(unique_agents) >= 3


@pytest.mark.unit
class TestApacheGenerator:
    def test_produces_valid_lines(self, sample_apache_logs):
        """Each line must match Apache combined log format."""
        with open(sample_apache_logs) as f:
            lines = f.readlines()

        assert len(lines) == 50

        for line in lines:
            match = APACHE_PATTERN.match(line.strip())
            assert match is not None, f"Line does not match Apache format: {line.strip()}"

    def test_deterministic(self, tmp_output_dir):
        """Same seed produces identical output."""
        path1 = os.path.join(tmp_output_dir, "adet1.log")
        path2 = os.path.join(tmp_output_dir, "adet2.log")

        generate_apache_logs(path1, num_lines=100, seed=42)
        generate_apache_logs(path2, num_lines=100, seed=42)

        with open(path1) as f1, open(path2) as f2:
            assert f1.read() == f2.read()
