"""Tests for the ingestion module: extract_metrics and generate_sample_logs."""

from __future__ import annotations

import time

import pytest

from src.ingestion import extract_metrics, generate_sample_logs
from src.models import LogEntry, MetricPoint


# ------------------------------------------------------------------ #
#  extract_metrics tests
# ------------------------------------------------------------------ #


class TestExtractResponseTime:
    """test_extract_response_time: log with response_time produces metric."""

    def test_extract_response_time(self) -> None:
        log = LogEntry(
            timestamp=1000.0,
            service="svc",
            level="INFO",
            response_time=150.0,
            method="GET",
            endpoint="/api/users",
            status_code=200,
        )
        metrics = extract_metrics([log])
        rt_metrics = [m for m in metrics if m.metric_name == "response_time"]
        assert len(rt_metrics) == 1
        assert rt_metrics[0].value == 150.0
        assert rt_metrics[0].service == "svc"
        assert rt_metrics[0].timestamp == 1000.0


class TestExtractErrorCount:
    """test_extract_error_count: log with level=ERROR produces error_count."""

    def test_extract_error_count(self) -> None:
        log = LogEntry(
            timestamp=2000.0,
            service="svc",
            level="ERROR",
            error_type="TimeoutError",
            endpoint="/api/orders",
        )
        metrics = extract_metrics([log])
        err_metrics = [m for m in metrics if m.metric_name == "error_count"]
        assert len(err_metrics) == 1
        assert err_metrics[0].value == 1.0
        assert err_metrics[0].tags.get("error_type") == "TimeoutError"


class TestExtractRequestCount:
    """test_extract_request_count: log with method produces request_count."""

    def test_extract_request_count(self) -> None:
        log = LogEntry(
            timestamp=3000.0,
            service="svc",
            level="INFO",
            method="POST",
            endpoint="/api/products",
            status_code=201,
        )
        metrics = extract_metrics([log])
        req_metrics = [m for m in metrics if m.metric_name == "request_count"]
        assert len(req_metrics) == 1
        assert req_metrics[0].value == 1.0
        assert req_metrics[0].tags.get("method") == "POST"


class TestExtractMultipleMetrics:
    """test_extract_multiple_metrics: a single log can produce several metrics."""

    def test_extract_multiple_metrics(self) -> None:
        log = LogEntry(
            timestamp=4000.0,
            service="svc",
            level="INFO",
            response_time=100.0,
            method="GET",
            endpoint="/api/health",
            status_code=200,
        )
        metrics = extract_metrics([log])
        names = {m.metric_name for m in metrics}
        # Should produce both response_time and request_count
        assert "response_time" in names
        assert "request_count" in names
        assert len(metrics) >= 2


class TestExtractEmptyLogs:
    """test_extract_empty_logs: empty list returns empty list."""

    def test_extract_empty_logs(self) -> None:
        assert extract_metrics([]) == []


class TestExtractNoMetricLog:
    """test_extract_no_metric_log: log with no extractable fields returns empty."""

    def test_extract_no_metric_log(self) -> None:
        log = LogEntry(
            timestamp=5000.0,
            service="svc",
            level="INFO",
            message="Just a plain info log",
        )
        metrics = extract_metrics([log])
        assert metrics == []


class TestTagsPopulated:
    """test_tags_populated: tags include endpoint, method, status_code when present."""

    def test_tags_populated(self) -> None:
        log = LogEntry(
            timestamp=6000.0,
            service="svc",
            level="INFO",
            response_time=80.0,
            method="PUT",
            endpoint="/api/auth",
            status_code=200,
        )
        metrics = extract_metrics([log])
        rt_metric = next(m for m in metrics if m.metric_name == "response_time")
        assert rt_metric.tags["endpoint"] == "/api/auth"
        assert rt_metric.tags["method"] == "PUT"
        assert rt_metric.tags["status_code"] == "200"


class TestTagsExcludeNone:
    """test_tags_exclude_none: None values are not included in tags."""

    def test_tags_exclude_none(self) -> None:
        log = LogEntry(
            timestamp=7000.0,
            service="svc",
            level="INFO",
            response_time=90.0,
            # method is None, status_code is None
            endpoint="/api/users",
        )
        metrics = extract_metrics([log])
        rt_metric = next(m for m in metrics if m.metric_name == "response_time")
        assert "method" not in rt_metric.tags
        assert "status_code" not in rt_metric.tags
        assert rt_metric.tags["endpoint"] == "/api/users"


# ------------------------------------------------------------------ #
#  generate_sample_logs tests
# ------------------------------------------------------------------ #


class TestGenerateCount:
    """test_generate_count: generates the correct number of logs."""

    def test_generate_count(self) -> None:
        logs = generate_sample_logs(count=25)
        assert len(logs) == 25

    def test_generate_count_default(self) -> None:
        logs = generate_sample_logs()
        assert len(logs) == 50


class TestGenerateServiceName:
    """test_generate_service_name: all logs carry the correct service name."""

    def test_generate_service_name(self) -> None:
        logs = generate_sample_logs(service="my-svc", count=20)
        assert all(log.service == "my-svc" for log in logs)


class TestGenerateHasVariety:
    """test_generate_has_variety: generated logs contain multiple methods and endpoints."""

    def test_generate_has_variety(self) -> None:
        logs = generate_sample_logs(count=200)
        methods = {log.method for log in logs if log.method}
        endpoints = {log.endpoint for log in logs if log.endpoint}
        assert len(methods) >= 2, f"Expected variety in methods, got {methods}"
        assert len(endpoints) >= 2, f"Expected variety in endpoints, got {endpoints}"


class TestGenerateHasErrors:
    """test_generate_has_errors: some logs have level=ERROR."""

    def test_generate_has_errors(self) -> None:
        logs = generate_sample_logs(count=200)
        error_logs = [log for log in logs if log.level == "ERROR"]
        assert len(error_logs) > 0, "Expected some ERROR logs in a large sample"


class TestGenerateTimestampsRecent:
    """test_generate_timestamps_recent: all timestamps within the last 6 minutes."""

    def test_generate_timestamps_recent(self) -> None:
        now = time.time()
        logs = generate_sample_logs(count=50)
        six_minutes_ago = now - 360  # 6 minutes
        for log in logs:
            assert log.timestamp >= six_minutes_ago, (
                f"Timestamp {log.timestamp} is older than 6 minutes ago"
            )
            assert log.timestamp <= now + 1, (
                f"Timestamp {log.timestamp} is in the future"
            )
