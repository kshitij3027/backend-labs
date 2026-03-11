"""Tests for Pydantic models."""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.models import LogEntry, ConsumerStats, EndpointMetrics, DashboardStats


def test_log_entry_valid():
    """LogEntry accepts valid data."""
    entry = LogEntry(
        ip="192.168.1.1",
        method="GET",
        path="/api/users",
        status_code=200,
        response_size=1234,
        response_time_ms=45.2,
        raw='192.168.1.1 - - [10/Oct/2023:13:55:36 +0000] "GET /api/users HTTP/1.1" 200 1234',
    )
    assert entry.ip == "192.168.1.1"
    assert entry.status_code == 200
    assert entry.response_time_ms == 45.2


def test_log_entry_optional_fields():
    """LogEntry works without optional fields."""
    entry = LogEntry(
        ip="10.0.0.1",
        method="POST",
        path="/api/orders",
        status_code=201,
        response_size=567,
        raw="test raw line",
    )
    assert entry.response_time_ms is None
    assert entry.timestamp is None


def test_log_entry_rejects_invalid():
    """LogEntry rejects missing required fields."""
    with pytest.raises(ValidationError):
        LogEntry(ip="1.2.3.4", method="GET")  # missing path, status_code, response_size, raw


def test_consumer_stats_defaults():
    """ConsumerStats has sensible defaults."""
    stats = ConsumerStats(consumer_id="worker-1")
    assert stats.processed_count == 0
    assert stats.error_count == 0
    assert stats.success_rate == 1.0
    assert stats.last_active is None


def test_endpoint_metrics():
    """EndpointMetrics validates correctly."""
    metrics = EndpointMetrics(
        path="/api/users",
        request_count=100,
        avg_response_time=45.2,
        error_rate=0.05,
        p50=40.0,
        p95=80.0,
        p99=120.0,
    )
    assert metrics.path == "/api/users"
    assert metrics.request_count == 100


def test_dashboard_stats_defaults():
    """DashboardStats has empty defaults."""
    stats = DashboardStats()
    assert stats.total_processed == 0
    assert stats.consumers == []
    assert stats.endpoints == {}
    assert stats.status_code_distribution == {}
    assert stats.top_paths == []


def test_dashboard_stats_with_data():
    """DashboardStats accepts nested data."""
    stats = DashboardStats(
        total_processed=100,
        total_errors=5,
        requests_per_second=10.5,
        consumers=[
            ConsumerStats(consumer_id="w1", processed_count=50),
            ConsumerStats(consumer_id="w2", processed_count=50),
        ],
        status_code_distribution={"200": 90, "500": 10},
        uptime_seconds=3600.0,
    )
    assert len(stats.consumers) == 2
    assert stats.status_code_distribution["200"] == 90
