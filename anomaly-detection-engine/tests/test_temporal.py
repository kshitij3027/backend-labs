"""Tests for the Temporal Pattern anomaly detector."""
from __future__ import annotations

import numpy as np
import pytest

from src.detectors.base import BaseDetector
from src.detectors.temporal import TemporalPatternDetector
from src.pipeline.feature_extractor import FeatureExtractor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_FEATURES = FeatureExtractor.NUM_FEATURES  # 9


def _make_features(
    hour: float = 12.0,
    weekday: float = 2.0,
    response_time: float = 200.0,
) -> np.ndarray:
    """Build a feature vector with explicit hour, weekday, and response_time.

    Other feature slots are zero-filled since the temporal detector only
    inspects indices 0, 1, and 3.
    """
    vec = np.zeros(NUM_FEATURES, dtype=np.float64)
    vec[0] = hour
    vec[1] = weekday
    vec[3] = response_time
    return vec


def _warm_up_bucket(
    detector: TemporalPatternDetector,
    hour: float,
    weekday: float,
    mean_rt: float,
    std_rt: float,
    n: int = 20,
    rng: np.random.Generator | None = None,
) -> None:
    """Feed *n* observations into a specific hour/weekday bucket."""
    if rng is None:
        rng = np.random.default_rng(42)
    for _ in range(n):
        rt = rng.normal(loc=mean_rt, scale=std_rt)
        detector.update(_make_features(hour=hour, weekday=weekday, response_time=rt))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTemporalPatternDetector:
    """Unit tests for TemporalPatternDetector."""

    def test_implements_base_interface(self) -> None:
        """TemporalPatternDetector should be a subclass of BaseDetector."""
        assert issubclass(TemporalPatternDetector, BaseDetector)

    def test_name_property(self) -> None:
        """The name property must return 'temporal'."""
        assert TemporalPatternDetector().name == "temporal"

    def test_detector_starts_not_ready(self) -> None:
        """A freshly created detector must not be ready."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        assert detector.is_ready() is False

    def test_detector_becomes_ready(self) -> None:
        """After sufficient updates, is_ready returns True."""
        min_bucket = 5
        detector = TemporalPatternDetector(min_bucket_size=min_bucket)
        rng = np.random.default_rng(0)

        # Need min_bucket_size * 2 = 10 total updates
        required = min_bucket * 2
        for i in range(required - 1):
            rt = rng.normal(loc=200.0, scale=20.0)
            detector.update(_make_features(hour=12.0, weekday=2.0, response_time=rt))
            assert detector.is_ready() is False, f"Should not be ready at {i + 1} updates"

        # One more pushes it over
        detector.update(
            _make_features(hour=12.0, weekday=2.0, response_time=200.0)
        )
        assert detector.is_ready() is True

    def test_normal_response_time_low_score(self) -> None:
        """A response time close to the baseline should produce a low score."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(42)

        # Build baseline at hour=14, weekday=3 with RT ~200
        _warm_up_bucket(detector, hour=14.0, weekday=3.0, mean_rt=200.0, std_rt=20.0, n=20, rng=rng)

        # Detect with a value very close to the mean
        result = detector.detect(_make_features(hour=14.0, weekday=3.0, response_time=210.0))

        assert result.score < 0.3, f"Expected low score for normal RT, got {result.score}"
        assert result.name == "temporal"

    def test_abnormal_response_time_high_score(self) -> None:
        """A response time far from the baseline should produce a high score (> 0.8)."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(42)

        # Build baseline at hour=10, weekday=1 with RT ~200
        _warm_up_bucket(detector, hour=10.0, weekday=1.0, mean_rt=200.0, std_rt=20.0, n=20, rng=rng)

        # Detect with an extreme value
        result = detector.detect(_make_features(hour=10.0, weekday=1.0, response_time=5000.0))

        assert result.score > 0.8, f"Expected high score for extreme RT, got {result.score}"
        assert result.name == "temporal"

    def test_different_hours_different_baselines(self) -> None:
        """Hour=9 baseline ~200 and hour=3 baseline ~500; RT=200 at hour=3 should be anomalous."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(99)

        # Use the same weekday for both so weekday doesn't dominate
        weekday = 2.0

        # Hour 9: fast responses (~200ms)
        _warm_up_bucket(detector, hour=9.0, weekday=weekday, mean_rt=200.0, std_rt=15.0, n=20, rng=rng)
        # Hour 3: slow responses (~500ms)
        _warm_up_bucket(detector, hour=3.0, weekday=weekday, mean_rt=500.0, std_rt=15.0, n=20, rng=rng)

        # RT=200 is normal for hour 9 but abnormal for hour 3
        result_normal = detector.detect(_make_features(hour=9.0, weekday=weekday, response_time=200.0))
        result_anomaly = detector.detect(_make_features(hour=3.0, weekday=weekday, response_time=200.0))

        assert result_anomaly.score > result_normal.score, (
            f"RT=200 at hour 3 ({result_anomaly.score:.3f}) should score higher than "
            f"RT=200 at hour 9 ({result_normal.score:.3f})"
        )
        # The anomaly at hour 3 should be quite strong (300ms deviation from mean 500, std ~15)
        assert result_anomaly.score > 0.8, (
            f"Expected high anomaly score for RT=200 at hour 3, got {result_anomaly.score}"
        )

    def test_weekday_vs_weekend_patterns(self) -> None:
        """Weekday=0 (Mon) baseline ~200 and weekday=5 (Sat) baseline ~500; detection varies."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(77)

        hour = 12.0

        # Monday: fast responses (~200ms)
        _warm_up_bucket(detector, hour=hour, weekday=0.0, mean_rt=200.0, std_rt=15.0, n=20, rng=rng)
        # Saturday: slow responses (~500ms)
        _warm_up_bucket(detector, hour=hour, weekday=5.0, mean_rt=500.0, std_rt=15.0, n=20, rng=rng)

        # RT=200 should be normal on Monday but anomalous on Saturday
        result_monday = detector.detect(_make_features(hour=hour, weekday=0.0, response_time=200.0))
        result_saturday = detector.detect(_make_features(hour=hour, weekday=5.0, response_time=200.0))

        assert result_saturday.score > result_monday.score, (
            f"RT=200 on Saturday ({result_saturday.score:.3f}) should score higher than "
            f"on Monday ({result_monday.score:.3f})"
        )
        assert result_saturday.score > 0.8, (
            f"Expected high anomaly score for RT=200 on Saturday, got {result_saturday.score}"
        )

    def test_not_ready_returns_zero(self) -> None:
        """Before enough data, detect() must return score=0.0 with ready=False."""
        detector = TemporalPatternDetector(min_bucket_size=10)

        # Feed a few samples -- not enough for the bucket
        for _ in range(3):
            detector.update(_make_features(hour=12.0, weekday=2.0, response_time=200.0))

        result = detector.detect(_make_features(hour=12.0, weekday=2.0, response_time=200.0))

        assert result.score == 0.0
        assert result.name == "temporal"
        assert result.details.get("ready") is False

    def test_details_contain_expected_keys(self) -> None:
        """The details dict must contain all expected deviation and context keys."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(42)

        _warm_up_bucket(detector, hour=10.0, weekday=3.0, mean_rt=200.0, std_rt=20.0, n=20, rng=rng)

        result = detector.detect(_make_features(hour=10.0, weekday=3.0, response_time=300.0))

        expected_keys = {
            "hourly_deviation",
            "weekday_deviation",
            "hourly_mean",
            "weekday_mean",
            "current_hour",
            "current_weekday",
        }
        assert expected_keys.issubset(result.details.keys()), (
            f"Missing keys: {expected_keys - set(result.details.keys())}"
        )
        # Verify types
        assert isinstance(result.details["hourly_deviation"], float)
        assert isinstance(result.details["weekday_deviation"], float)
        assert isinstance(result.details["hourly_mean"], float)
        assert isinstance(result.details["weekday_mean"], float)
        assert result.details["current_hour"] == 10
        assert result.details["current_weekday"] == 3

    def test_confidence_clamped_0_1(self) -> None:
        """Confidence must stay within [0.0, 1.0] even for extreme deviations."""
        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(42)

        _warm_up_bucket(detector, hour=8.0, weekday=1.0, mean_rt=200.0, std_rt=10.0, n=20, rng=rng)

        # Extreme response time
        result = detector.detect(_make_features(hour=8.0, weekday=1.0, response_time=1_000_000.0))

        assert 0.0 <= result.score <= 1.0, f"Score out of range: {result.score}"
        assert result.score == 1.0, "Extreme value should hit max confidence"

    def test_thread_safety(self) -> None:
        """Concurrent updates and detects should not raise or corrupt state."""
        import threading

        detector = TemporalPatternDetector(min_bucket_size=5)
        rng = np.random.default_rng(123)
        errors: list[Exception] = []

        def updater() -> None:
            try:
                for _ in range(50):
                    rt = rng.normal(loc=200.0, scale=20.0)
                    detector.update(_make_features(hour=12.0, weekday=3.0, response_time=rt))
            except Exception as exc:
                errors.append(exc)

        def detector_fn() -> None:
            try:
                for _ in range(50):
                    detector.detect(_make_features(hour=12.0, weekday=3.0, response_time=200.0))
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=updater),
            threading.Thread(target=updater),
            threading.Thread(target=detector_fn),
            threading.Thread(target=detector_fn),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread safety errors: {errors}"
