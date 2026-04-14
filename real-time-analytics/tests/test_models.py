"""Tests for Pydantic models."""

from __future__ import annotations

import time

import pytest
from pydantic import ValidationError

from src.models import (
    AnomalyRecord,
    AnomalyResponse,
    HealthResponse,
    LogEntry,
    MetricPoint,
    MetricResponse,
)


# ------------------------------------------------------------------
# LogEntry
# ------------------------------------------------------------------


class TestLogEntry:
    def test_defaults(self):
        entry = LogEntry(timestamp=1000.0)
        assert entry.timestamp == 1000.0
        assert entry.service == "unknown"
        assert entry.level == "INFO"
        assert entry.message == ""
        assert entry.response_time is None
        assert entry.method is None
        assert entry.endpoint is None
        assert entry.status_code is None
        assert entry.error_type is None
        assert entry.tags == {}

    def test_all_fields(self):
        entry = LogEntry(
            timestamp=1000.0,
            service="api-gateway",
            level="ERROR",
            message="connection refused",
            response_time=1.23,
            method="POST",
            endpoint="/v1/users",
            status_code=502,
            error_type="ConnectionError",
            tags={"region": "us-east-1"},
        )
        assert entry.service == "api-gateway"
        assert entry.level == "ERROR"
        assert entry.message == "connection refused"
        assert entry.response_time == 1.23
        assert entry.method == "POST"
        assert entry.endpoint == "/v1/users"
        assert entry.status_code == 502
        assert entry.error_type == "ConnectionError"
        assert entry.tags == {"region": "us-east-1"}

    def test_optional_fields_none_by_default(self):
        entry = LogEntry(timestamp=1.0)
        for field in ("response_time", "method", "endpoint", "status_code", "error_type"):
            assert getattr(entry, field) is None


# ------------------------------------------------------------------
# MetricPoint
# ------------------------------------------------------------------


class TestMetricPoint:
    def test_required_fields(self):
        mp = MetricPoint(
            service="web",
            metric_name="cpu",
            value=75.5,
            timestamp=1000.0,
        )
        assert mp.service == "web"
        assert mp.metric_name == "cpu"
        assert mp.value == 75.5
        assert mp.timestamp == 1000.0

    def test_tags_default_empty_dict(self):
        mp = MetricPoint(
            service="web",
            metric_name="cpu",
            value=1.0,
            timestamp=1.0,
        )
        assert mp.tags == {}

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            MetricPoint(service="web", value=1.0, timestamp=1.0)  # missing metric_name


# ------------------------------------------------------------------
# AnomalyRecord
# ------------------------------------------------------------------


class TestAnomalyRecord:
    def test_all_fields(self):
        ar = AnomalyRecord(
            service="payments",
            metric_name="error_rate",
            value=0.45,
            z_score=3.2,
            threshold=2.5,
            mean=0.05,
            std=0.125,
            timestamp=1000.0,
        )
        assert ar.service == "payments"
        assert ar.metric_name == "error_rate"
        assert ar.value == 0.45
        assert ar.z_score == 3.2
        assert ar.threshold == 2.5
        assert ar.mean == 0.05
        assert ar.std == 0.125
        assert ar.timestamp == 1000.0

    def test_is_anomaly_default_true(self):
        ar = AnomalyRecord(
            service="s",
            metric_name="m",
            value=1.0,
            z_score=3.0,
            threshold=2.5,
            mean=0.5,
            std=0.1,
            timestamp=1.0,
        )
        assert ar.is_anomaly is True

    def test_is_anomaly_can_be_false(self):
        ar = AnomalyRecord(
            service="s",
            metric_name="m",
            value=1.0,
            z_score=1.0,
            threshold=2.5,
            mean=0.5,
            std=0.5,
            timestamp=1.0,
            is_anomaly=False,
        )
        assert ar.is_anomaly is False

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            AnomalyRecord(service="s", metric_name="m")  # missing many required fields


# ------------------------------------------------------------------
# MetricResponse
# ------------------------------------------------------------------


class TestMetricResponse:
    def test_empty_data_points(self):
        mr = MetricResponse(
            service="api",
            metric_name="latency",
            data_points=[],
            count=0,
        )
        assert mr.data_points == []
        assert mr.count == 0

    def test_trend_none_by_default(self):
        mr = MetricResponse(
            service="api",
            metric_name="latency",
            data_points=[],
            count=0,
        )
        assert mr.trend is None

    def test_with_data_points_and_trend(self):
        point = MetricPoint(
            service="api",
            metric_name="latency",
            value=100.0,
            timestamp=1000.0,
        )
        mr = MetricResponse(
            service="api",
            metric_name="latency",
            data_points=[point],
            count=1,
            trend={"slope": 0.5, "direction": "up"},
        )
        assert len(mr.data_points) == 1
        assert mr.trend["slope"] == 0.5


# ------------------------------------------------------------------
# AnomalyResponse
# ------------------------------------------------------------------


class TestAnomalyResponse:
    def test_count_and_hours(self):
        ar = AnomalyResponse(anomalies=[], count=0, hours=1.0)
        assert ar.count == 0
        assert ar.hours == 1.0

    def test_with_anomalies(self):
        record = AnomalyRecord(
            service="s",
            metric_name="m",
            value=5.0,
            z_score=4.0,
            threshold=2.5,
            mean=1.0,
            std=1.0,
            timestamp=time.time(),
        )
        ar = AnomalyResponse(anomalies=[record], count=1, hours=2.0)
        assert ar.count == 1
        assert ar.hours == 2.0
        assert len(ar.anomalies) == 1
