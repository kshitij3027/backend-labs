"""Tests for src.metrics — in-memory metrics aggregation."""

from __future__ import annotations

import pytest
import pytest_asyncio

from src.metrics import MetricsAggregator, _percentiles
from src.models import LogEntry


def _make_entry(
    path: str = "/api/test",
    status_code: int = 200,
    response_time_ms: float | None = 50.0,
    ip: str = "10.0.0.1",
    method: str = "GET",
    response_size: int = 100,
) -> LogEntry:
    """Helper to build a LogEntry for testing."""
    return LogEntry(
        ip=ip,
        method=method,
        path=path,
        status_code=status_code,
        response_size=response_size,
        response_time_ms=response_time_ms,
        timestamp=None,
        raw=f'{ip} - - [10/Mar/2026:00:00:00 +0000] "{method} {path} HTTP/1.1" {status_code} {response_size}',
    )


@pytest.mark.asyncio
async def test_record_and_snapshot():
    """Record a single entry and verify it shows up in the snapshot."""
    agg = MetricsAggregator(window_sec=300)
    entry = _make_entry()
    await agg.record(entry)

    snap = await agg.snapshot()
    assert snap.total_processed == 1
    assert snap.total_errors == 0
    assert "/api/test" in snap.endpoints
    assert snap.endpoints["/api/test"].request_count == 1


@pytest.mark.asyncio
async def test_avg_response_time():
    """Average response time across multiple entries."""
    agg = MetricsAggregator(window_sec=300)
    for rt in [10.0, 20.0, 30.0]:
        await agg.record(_make_entry(response_time_ms=rt))

    snap = await agg.snapshot()
    ep = snap.endpoints["/api/test"]
    assert ep.avg_response_time == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_status_code_distribution():
    """Mix of 200/404/500 shows correct distribution."""
    agg = MetricsAggregator(window_sec=300)
    for code in [200, 200, 200, 404, 500]:
        await agg.record(_make_entry(status_code=code))

    snap = await agg.snapshot()
    assert snap.status_code_distribution["200"] == 3
    assert snap.status_code_distribution["404"] == 1
    assert snap.status_code_distribution["500"] == 1
    assert snap.total_errors == 2  # 404 + 500


@pytest.mark.asyncio
async def test_top_paths():
    """Top paths ordered by request count descending."""
    agg = MetricsAggregator(window_sec=300)
    for _ in range(5):
        await agg.record(_make_entry(path="/popular"))
    for _ in range(3):
        await agg.record(_make_entry(path="/medium"))
    for _ in range(1):
        await agg.record(_make_entry(path="/rare"))

    snap = await agg.snapshot()
    assert len(snap.top_paths) == 3
    assert snap.top_paths[0]["path"] == "/popular"
    assert snap.top_paths[0]["count"] == 5
    assert snap.top_paths[1]["path"] == "/medium"
    assert snap.top_paths[1]["count"] == 3
    assert snap.top_paths[2]["path"] == "/rare"
    assert snap.top_paths[2]["count"] == 1


@pytest.mark.asyncio
async def test_percentiles():
    """Verify p50/p95/p99 calculation with known data."""
    agg = MetricsAggregator(window_sec=300)
    # Insert 100 entries with response times 1..100
    for i in range(1, 101):
        await agg.record(_make_entry(response_time_ms=float(i)))

    snap = await agg.snapshot()
    # Index-based: p50 -> index 50 of sorted 1..100 = 51
    assert snap.latency_percentiles["p50"] == pytest.approx(51.0)
    # p95 -> index 95 = 96
    assert snap.latency_percentiles["p95"] == pytest.approx(96.0)
    # p99 -> index 99 = 100
    assert snap.latency_percentiles["p99"] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_requests_per_second():
    """Requests per second is total_processed / elapsed time."""
    agg = MetricsAggregator(window_sec=300)
    for _ in range(10):
        await agg.record(_make_entry())

    snap = await agg.snapshot()
    assert snap.total_processed == 10
    assert snap.requests_per_second > 0
    assert snap.uptime_seconds > 0
    # rps = total / elapsed; since elapsed is tiny, rps should be very high
    assert snap.requests_per_second == pytest.approx(
        snap.total_processed / snap.uptime_seconds, rel=0.1
    )


@pytest.mark.asyncio
async def test_top_ips():
    """Top IPs ordered by count."""
    agg = MetricsAggregator(window_sec=300)
    for _ in range(4):
        await agg.record(_make_entry(ip="1.1.1.1"))
    for _ in range(2):
        await agg.record(_make_entry(ip="2.2.2.2"))
    await agg.record(_make_entry(ip="3.3.3.3"))

    snap = await agg.snapshot()
    assert snap.top_ips[0]["ip"] == "1.1.1.1"
    assert snap.top_ips[0]["count"] == 4
    assert snap.top_ips[1]["ip"] == "2.2.2.2"
    assert snap.top_ips[1]["count"] == 2


class TestPercentileHelper:
    """Unit tests for the _percentiles helper function."""

    def test_empty_values(self):
        assert _percentiles([], [0.5, 0.95, 0.99]) == (0.0, 0.0, 0.0)

    def test_single_value(self):
        assert _percentiles([42.0], [0.5, 0.95, 0.99]) == (42.0, 42.0, 42.0)

    def test_known_values(self):
        vals = list(range(1, 101))  # 1..100
        p50, p95, p99 = _percentiles([float(v) for v in vals], [0.50, 0.95, 0.99])
        assert p50 == 51.0
        assert p95 == 96.0
        assert p99 == 100.0
