"""Tests for GeoAnalyzer."""

import threading

import pytest

from src.geo_analyzer import GeoAnalyzer


@pytest.fixture
def geo():
    return GeoAnalyzer()


# ── IP analysis ──────────────────────────────────────────────────

def test_analyze_ip_returns_structure(geo):
    result = geo.analyze_ip("8.8.8.8")
    assert "country" in result
    assert "region" in result
    assert "city" in result
    assert "lat" in result
    assert "lon" in result


def test_analyze_ip_deterministic(geo):
    """Same IP should always map to the same region."""
    r1 = geo.analyze_ip("203.0.113.50")
    # Create a fresh analyzer to bypass cache
    geo2 = GeoAnalyzer()
    r2 = geo2.analyze_ip("203.0.113.50")
    assert r1["country"] == r2["country"]
    assert r1["city"] == r2["city"]


def test_analyze_private_ip(geo):
    result = geo.analyze_ip("10.0.0.1")
    assert result["country"] == "Internal"
    assert result["region"] == "Internal"


def test_analyze_private_ip_172(geo):
    result = geo.analyze_ip("172.16.0.1")
    assert result["region"] == "Internal"


def test_analyze_private_ip_192(geo):
    result = geo.analyze_ip("192.168.1.1")
    assert result["region"] == "Internal"


def test_analyze_ip_caching(geo):
    """Second call should use cache and return the same object."""
    r1 = geo.analyze_ip("8.8.8.8")
    r2 = geo.analyze_ip("8.8.8.8")
    assert r1["country"] == r2["country"]
    assert r1["city"] == r2["city"]


def test_analyze_none_ip(geo):
    result = geo.analyze_ip(None)
    assert result is None


def test_analyze_empty_ip(geo):
    result = geo.analyze_ip("")
    assert result is None


# ── Latency recording ───────────────────────────────────────────

def test_record_latency(geo):
    geo.record_latency("8.8.8.8", 150.0)
    geo.record_latency("8.8.8.8", 200.0)
    metrics = geo.get_geo_metrics()
    latency = metrics["latency_by_region"]
    # Should have at least one region with latency data
    assert len(latency) > 0
    for region, data in latency.items():
        assert "avg" in data
        assert "count" in data
        assert data["count"] >= 1


def test_latency_bounded(geo):
    """Latency list per region should not exceed 1000."""
    for i in range(1100):
        geo.record_latency("8.8.8.8", float(i))
    metrics = geo.get_geo_metrics()
    for region, data in metrics["latency_by_region"].items():
        assert data["count"] <= 1000


# ── Geo metrics structure ────────────────────────────────────────

def test_get_geo_metrics_structure(geo):
    metrics = geo.get_geo_metrics()
    assert "traffic_by_region" in metrics
    assert "latency_by_region" in metrics


def test_traffic_increments(geo):
    geo.analyze_ip("8.8.8.8")
    geo.analyze_ip("8.8.8.8")
    geo.analyze_ip("8.8.8.8")
    metrics = geo.get_geo_metrics()
    total = sum(metrics["traffic_by_region"].values())
    assert total == 3


# ── Thread safety ────────────────────────────────────────────────

def test_thread_safety(geo):
    errors = []

    def worker():
        try:
            for i in range(200):
                geo.analyze_ip(f"1.2.3.{i % 256}")
                geo.record_latency(f"1.2.3.{i % 256}", float(i))
                geo.get_geo_metrics()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
