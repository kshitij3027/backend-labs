"""Tests for RedisStorage using fakeredis."""

from __future__ import annotations

import time

import pytest

from src.models import AnomalyRecord, MetricPoint
from src.storage import RedisStorage


# ------------------------------------------------------------------
# Metric tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_and_get_metric(storage: RedisStorage):
    """Store one metric, retrieve by time range, verify roundtrip."""
    now = time.time()
    point = MetricPoint(
        service="web",
        metric_name="cpu",
        value=72.5,
        timestamp=now,
        tags={"host": "node-1"},
    )
    await storage.store_metric(point)

    results = await storage.get_metrics("web", "cpu", now - 10, now + 10)
    assert len(results) == 1
    assert results[0].service == "web"
    assert results[0].metric_name == "cpu"
    assert results[0].value == 72.5
    assert results[0].timestamp == now
    assert results[0].tags == {"host": "node-1"}


@pytest.mark.asyncio
async def test_store_metrics_batch(storage: RedisStorage):
    """Batch store 10 points, verify count and retrieval."""
    base_ts = 1_000_000.0
    points = [
        MetricPoint(
            service="api",
            metric_name="latency",
            value=float(i * 10),
            timestamp=base_ts + i,
        )
        for i in range(10)
    ]
    await storage.store_metrics_batch(points)

    results = await storage.get_metrics("api", "latency", base_ts - 1, base_ts + 20)
    assert len(results) == 10
    values = [r.value for r in results]
    assert values == [float(i * 10) for i in range(10)]


@pytest.mark.asyncio
async def test_get_metrics_empty(storage: RedisStorage):
    """Query with no data returns empty list."""
    results = await storage.get_metrics("nonexistent", "metric", 0, time.time())
    assert results == []


@pytest.mark.asyncio
async def test_get_metrics_time_range(storage: RedisStorage):
    """Store points at different timestamps, verify range filter works."""
    base = 1_000_000.0
    for i in range(5):
        point = MetricPoint(
            service="svc",
            metric_name="mem",
            value=float(i),
            timestamp=base + (i * 100),  # 1000000, 1000100, 1000200, 1000300, 1000400
        )
        await storage.store_metric(point)

    # Query range that covers only indices 1, 2, 3 (timestamps 1000100..1000300)
    results = await storage.get_metrics("svc", "mem", base + 100, base + 300)
    assert len(results) == 3
    values = [r.value for r in results]
    assert values == [1.0, 2.0, 3.0]


# ------------------------------------------------------------------
# Anomaly tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_store_and_get_anomaly(storage: RedisStorage):
    """Store anomaly, retrieve via get_anomalies."""
    now = time.time()
    record = AnomalyRecord(
        service="payments",
        metric_name="error_rate",
        value=0.45,
        z_score=3.2,
        threshold=2.5,
        mean=0.05,
        std=0.125,
        timestamp=now,
    )
    await storage.store_anomaly(record)

    anomalies = await storage.get_anomalies(hours=1.0)
    assert len(anomalies) == 1
    assert anomalies[0].service == "payments"
    assert anomalies[0].metric_name == "error_rate"
    assert anomalies[0].value == 0.45
    assert anomalies[0].z_score == 3.2
    assert anomalies[0].is_anomaly is True


@pytest.mark.asyncio
async def test_get_anomalies_empty(storage: RedisStorage):
    """No anomalies returns empty list."""
    anomalies = await storage.get_anomalies(hours=1.0)
    assert anomalies == []


# ------------------------------------------------------------------
# Discovery helpers
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_services(storage: RedisStorage):
    """Store metrics for 2 services, verify both listed."""
    now = time.time()
    await storage.store_metric(MetricPoint(
        service="alpha", metric_name="cpu", value=1.0, timestamp=now,
    ))
    await storage.store_metric(MetricPoint(
        service="beta", metric_name="cpu", value=2.0, timestamp=now,
    ))

    services = await storage.get_services()
    assert "alpha" in services
    assert "beta" in services
    assert len(services) == 2


@pytest.mark.asyncio
async def test_get_metric_names(storage: RedisStorage):
    """Store different metric names for same service, verify all found."""
    now = time.time()
    for name in ("cpu", "memory", "disk_io"):
        await storage.store_metric(MetricPoint(
            service="worker", metric_name=name, value=50.0, timestamp=now,
        ))

    names = await storage.get_metric_names("worker")
    assert names == ["cpu", "disk_io", "memory"]  # sorted


# ------------------------------------------------------------------
# Connectivity
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping(storage: RedisStorage):
    """Verify ping returns True with fakeredis."""
    result = await storage.ping()
    assert result is True
