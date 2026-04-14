"""Tests for the anomaly detection engine."""

from __future__ import annotations

import time

import pytest

from src.engine.anomalies import RollingZScoreDetector, detect_anomalies
from src.models import MetricPoint


def _make_points(
    values: list[float],
    start_ts: float | None = None,
    interval: float = 1.0,
    service: str = "test-svc",
    metric_name: str = "test-metric",
) -> list[MetricPoint]:
    """Helper to build MetricPoints from raw values."""
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


class TestRollingZScoreDetector:
    """Tests for RollingZScoreDetector."""

    def test_obvious_outlier(self) -> None:
        """Normal values plus one extreme value should be detected as an anomaly."""
        values = [10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 10.0, 100.0]
        points = _make_points(values)
        detector = RollingZScoreDetector(threshold=2.5)
        anomalies = detector.detect(points)
        assert len(anomalies) >= 1
        # The last point (100.0) should be flagged
        anomaly_values = [a.value for a in anomalies]
        assert 100.0 in anomaly_values

    def test_normal_data_clean(self) -> None:
        """All similar values should produce no anomalies."""
        values = [50.0, 50.1, 49.9, 50.2, 49.8, 50.0, 50.1, 49.9, 50.0, 50.1]
        points = _make_points(values)
        detector = RollingZScoreDetector(threshold=2.5)
        anomalies = detector.detect(points)
        assert len(anomalies) == 0

    def test_threshold_sensitivity(self) -> None:
        """Lower threshold should catch more anomalies than higher threshold."""
        values = [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 15.0, 20.0]
        points = _make_points(values)

        strict = RollingZScoreDetector(threshold=3.0)
        lenient = RollingZScoreDetector(threshold=1.5)

        strict_anomalies = strict.detect(points)
        lenient_anomalies = lenient.detect(points)

        assert len(lenient_anomalies) >= len(strict_anomalies)

    def test_empty_data(self) -> None:
        """No data should return no anomalies."""
        detector = RollingZScoreDetector(threshold=2.5)
        anomalies = detector.detect([])
        assert anomalies == []

    def test_too_few_points(self) -> None:
        """Less than 3 points should return no anomalies."""
        points = _make_points([10.0, 20.0])
        detector = RollingZScoreDetector(threshold=2.5)
        anomalies = detector.detect(points)
        assert anomalies == []

    def test_identical_values(self) -> None:
        """All same value (std=0) should produce no anomalies and no ZeroDivisionError."""
        values = [5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0]
        points = _make_points(values)
        detector = RollingZScoreDetector(threshold=2.5)
        anomalies = detector.detect(points)
        assert anomalies == []

    def test_z_score_values(self) -> None:
        """Verify z_score field is populated and reasonable for detected anomalies."""
        values = [10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 50.0]
        points = _make_points(values)
        detector = RollingZScoreDetector(threshold=2.0)
        anomalies = detector.detect(points)
        assert len(anomalies) >= 1
        for a in anomalies:
            assert isinstance(a.z_score, float)
            assert abs(a.z_score) > detector.threshold
            assert a.is_anomaly is True
            assert a.mean > 0
            assert a.std > 0

    def test_detect_anomalies_convenience(self) -> None:
        """Test the wrapper function works the same as the class."""
        values = [10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 10.0, 10.1, 9.9, 100.0]
        points = _make_points(values)
        anomalies = detect_anomalies(points, threshold=2.5)
        assert len(anomalies) >= 1
        anomaly_values = [a.value for a in anomalies]
        assert 100.0 in anomaly_values
