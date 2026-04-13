"""Tests for the Z-score anomaly detector."""
from __future__ import annotations

import numpy as np
import pytest

from src.detectors.base import BaseDetector
from src.detectors.zscore import ZScoreDetector
from src.pipeline.feature_extractor import FeatureExtractor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_FEATURES = FeatureExtractor.NUM_FEATURES  # 9


def _gaussian_features(
    n: int,
    mean: float = 200.0,
    std: float = 50.0,
    rng: np.random.Generator | None = None,
) -> list[np.ndarray]:
    """Generate *n* feature vectors drawn from a Gaussian distribution."""
    if rng is None:
        rng = np.random.default_rng(42)
    return [rng.normal(loc=mean, scale=std, size=NUM_FEATURES) for _ in range(n)]


def _warm_up(
    detector: ZScoreDetector,
    n: int = 50,
    mean: float = 200.0,
    std: float = 50.0,
) -> None:
    """Feed *n* normal samples into the detector so it becomes ready."""
    for vec in _gaussian_features(n, mean=mean, std=std):
        detector.update(vec)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestZScoreDetector:
    """Unit tests for ZScoreDetector."""

    def test_implements_base_interface(self) -> None:
        """ZScoreDetector should be a subclass of BaseDetector."""
        assert issubclass(ZScoreDetector, BaseDetector)

    def test_detector_starts_not_ready(self) -> None:
        """A freshly created detector with an empty window must not be ready."""
        detector = ZScoreDetector()
        assert detector.is_ready() is False

    def test_detector_becomes_ready(self) -> None:
        """After adding min_ready_size entries the detector must report ready."""
        min_size = 30
        detector = ZScoreDetector(min_ready_size=min_size)
        rng = np.random.default_rng(0)

        for i in range(min_size - 1):
            detector.update(rng.normal(size=NUM_FEATURES))
            assert detector.is_ready() is False, f"Should not be ready at {i + 1} entries"

        detector.update(rng.normal(size=NUM_FEATURES))
        assert detector.is_ready() is True

    def test_normal_data_low_score(self) -> None:
        """A value close to the mean should produce a low anomaly score (< 0.3)."""
        detector = ZScoreDetector(threshold=3.0)
        _warm_up(detector, n=50, mean=200.0, std=50.0)

        # 210 is barely above the mean of 200 -- should not be anomalous
        normal_point = np.full(NUM_FEATURES, 210.0)
        result = detector.detect(normal_point)

        assert result.score < 0.3, f"Expected low score, got {result.score}"
        assert result.name == "zscore"

    def test_extreme_outlier_high_score(self) -> None:
        """A value far from the mean should produce a high anomaly score (> 0.8)."""
        detector = ZScoreDetector(threshold=3.0)
        _warm_up(detector, n=50, mean=200.0, std=50.0)

        # 1000 is many standard deviations away from 200
        outlier_point = np.full(NUM_FEATURES, 1000.0)
        result = detector.detect(outlier_point)

        assert result.score > 0.8, f"Expected high score, got {result.score}"
        assert result.name == "zscore"

    def test_confidence_clamped_0_1(self) -> None:
        """Confidence must stay within [0.0, 1.0] even for extreme z-scores."""
        detector = ZScoreDetector(threshold=3.0)
        _warm_up(detector, n=50, mean=200.0, std=50.0)

        extreme = np.full(NUM_FEATURES, 1_000_000.0)
        result = detector.detect(extreme)

        assert 0.0 <= result.score <= 1.0, f"Score out of range: {result.score}"

    def test_not_ready_returns_zero(self) -> None:
        """Before warm-up, detect() must return score=0.0 with ready=False."""
        detector = ZScoreDetector(min_ready_size=30)
        # Feed fewer than min_ready_size
        for vec in _gaussian_features(5):
            detector.update(vec)

        result = detector.detect(np.zeros(NUM_FEATURES))

        assert result.score == 0.0
        assert result.name == "zscore"
        assert result.details.get("ready") is False

    def test_threshold_configuration(self) -> None:
        """A lower threshold should yield higher confidence for the same outlier."""
        rng = np.random.default_rng(99)
        samples = [rng.normal(loc=200.0, scale=50.0, size=NUM_FEATURES) for _ in range(50)]

        det_low = ZScoreDetector(threshold=2.0)
        det_high = ZScoreDetector(threshold=5.0)

        for vec in samples:
            det_low.update(vec)
            det_high.update(vec)

        outlier = np.full(NUM_FEATURES, 400.0)
        score_low = det_low.detect(outlier).score
        score_high = det_high.detect(outlier).score

        assert score_low > score_high, (
            f"threshold=2.0 score ({score_low}) should exceed "
            f"threshold=5.0 score ({score_high})"
        )

    def test_details_contain_per_feature_z(self) -> None:
        """The details dict must contain a per_feature_z list of length 9."""
        detector = ZScoreDetector()
        _warm_up(detector, n=50)

        result = detector.detect(np.full(NUM_FEATURES, 500.0))
        per_z = result.details.get("per_feature_z")

        assert per_z is not None, "per_feature_z missing from details"
        assert len(per_z) == NUM_FEATURES, f"Expected {NUM_FEATURES} z-scores, got {len(per_z)}"
        assert all(isinstance(z, float) for z in per_z)

    def test_triggered_feature_name_valid(self) -> None:
        """The triggered_feature_name should match a name from FEATURE_NAMES."""
        detector = ZScoreDetector()
        _warm_up(detector, n=50, mean=200.0, std=50.0)

        # Make index 3 (response_time) the clear outlier
        point = np.full(NUM_FEATURES, 200.0)
        point[3] = 5000.0
        result = detector.detect(point)

        assert result.details["triggered_feature_idx"] == 3
        assert result.details["triggered_feature_name"] == "response_time"

    def test_name_property(self) -> None:
        """The name property must return 'zscore'."""
        assert ZScoreDetector().name == "zscore"
