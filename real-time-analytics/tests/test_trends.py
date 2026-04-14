"""Tests for the trend analysis engine."""

from __future__ import annotations

import time

import pytest

from src.engine.trends import calculate_trend
from src.models import MetricPoint


def _make_points(
    values: list[float],
    start_ts: float | None = None,
    interval: float = 10.0,
    service: str = "test-svc",
    metric_name: str = "test-metric",
) -> list[MetricPoint]:
    """Helper to build a list of MetricPoints from raw values."""
    if start_ts is None:
        start_ts = time.time() - (len(values) * interval)
    return [
        MetricPoint(
            service=service,
            metric_name=metric_name,
            value=v,
            timestamp=start_ts + i * interval,
        )
        for i, v in enumerate(values)
    ]


class TestCalculateTrend:
    """Tests for calculate_trend."""

    def test_increasing_trend(self) -> None:
        """10 increasing points should produce direction='increasing' with positive slope."""
        points = _make_points([10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0])
        result = calculate_trend(points, window_minutes=5.0)
        assert result["direction"] == "increasing"
        assert result["slope"] > 0
        assert result["data_points_count"] == 10

    def test_decreasing_trend(self) -> None:
        """10 decreasing points should produce direction='decreasing' with negative slope."""
        points = _make_points([100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0])
        result = calculate_trend(points, window_minutes=5.0)
        assert result["direction"] == "decreasing"
        assert result["slope"] < 0
        assert result["data_points_count"] == 10

    def test_stable_trend(self) -> None:
        """10 nearly-identical points should produce direction='stable'."""
        points = _make_points([50.0, 50.01, 49.99, 50.0, 50.01, 49.99, 50.0, 50.01, 49.99, 50.0])
        result = calculate_trend(points, window_minutes=5.0)
        assert result["direction"] == "stable"

    def test_insufficient_data_empty(self) -> None:
        """Empty data should return direction='insufficient_data'."""
        result = calculate_trend([], window_minutes=5.0)
        assert result["direction"] == "insufficient_data"
        assert result["slope"] == 0.0
        assert result["r_squared"] == 0.0
        assert result["change_rate"] == 0.0
        assert result["current_value"] == 0.0
        assert result["data_points_count"] == 0

    def test_insufficient_data_one(self) -> None:
        """One point should return direction='insufficient_data' with current_value set."""
        points = _make_points([42.5])
        result = calculate_trend(points, window_minutes=5.0)
        assert result["direction"] == "insufficient_data"
        assert result["current_value"] == 42.5
        assert result["data_points_count"] == 1

    def test_r_squared_range(self) -> None:
        """R-squared must be between 0 and 1."""
        points = _make_points([10.0, 12.0, 11.0, 14.0, 13.0, 16.0, 15.0, 18.0, 17.0, 20.0])
        result = calculate_trend(points, window_minutes=5.0)
        assert 0.0 <= result["r_squared"] <= 1.0

    def test_window_filtering(self) -> None:
        """Points outside the window should be excluded from analysis."""
        now = time.time()
        # 5 old points from 20 minutes ago
        old_points = _make_points(
            [100.0, 200.0, 300.0, 400.0, 500.0],
            start_ts=now - 1200,  # 20 minutes ago
            interval=10.0,
        )
        # 5 recent points within the last 2 minutes
        recent_points = _make_points(
            [10.0, 20.0, 30.0, 40.0, 50.0],
            start_ts=now - 100,
            interval=10.0,
        )
        all_points = old_points + recent_points
        result = calculate_trend(all_points, window_minutes=5.0)
        # Only the 5 recent points should be included
        assert result["data_points_count"] == 5
        assert result["direction"] == "increasing"
