"""Tests for the Isolation Forest anomaly detector."""
from __future__ import annotations

import numpy as np
import pytest

from src.detectors.isolation_forest import IsolationForestDetector


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

SEED = 42
NUM_FEATURES = 9
WARM_UP_SIZE = 150  # slightly above default so tests are explicit


@pytest.fixture()
def cluster_data() -> np.ndarray:
    """Generate a Gaussian cluster of 150 vectors with 9 features."""
    rng = np.random.default_rng(SEED)
    return rng.normal(loc=200, scale=50, size=(WARM_UP_SIZE, NUM_FEATURES))


@pytest.fixture()
def trained_detector(cluster_data: np.ndarray) -> IsolationForestDetector:
    """Return an IsolationForestDetector that has completed warm-up."""
    det = IsolationForestDetector(
        contamination=0.1,
        random_state=SEED,
        n_estimators=100,
        warm_up_size=WARM_UP_SIZE,
    )
    for row in cluster_data:
        det.update(row)
    return det


# -------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------


class TestIsolationForestDetector:
    """Unit tests for :class:`IsolationForestDetector`."""

    def test_detector_starts_not_ready(self) -> None:
        det = IsolationForestDetector(warm_up_size=WARM_UP_SIZE)
        assert det.is_ready() is False

    def test_detector_becomes_ready_after_warmup(
        self, trained_detector: IsolationForestDetector
    ) -> None:
        assert trained_detector.is_ready() is True

    def test_not_ready_returns_zero(self) -> None:
        det = IsolationForestDetector(warm_up_size=WARM_UP_SIZE)
        vec = np.zeros(NUM_FEATURES)
        result = det.detect(vec)
        assert result.score == 0.0
        assert result.name == "isolation_forest"
        assert result.details.get("ready") is False

    def test_normal_data_cluster_low_score(
        self, trained_detector: IsolationForestDetector, cluster_data: np.ndarray
    ) -> None:
        """A point close to the training cluster should produce a low score."""
        normal_point = cluster_data.mean(axis=0)  # centroid of the cluster
        result = trained_detector.detect(normal_point)
        assert result.score < 0.5, (
            f"Expected score < 0.5 for a normal point, got {result.score}"
        )

    def test_outlier_point_high_score(
        self, trained_detector: IsolationForestDetector
    ) -> None:
        """A point far from the training cluster should produce a high score."""
        outlier = np.full(NUM_FEATURES, 1000.0)
        result = trained_detector.detect(outlier)
        assert result.score > 0.5, (
            f"Expected score > 0.5 for an outlier, got {result.score}"
        )

    def test_scaler_applied(
        self, trained_detector: IsolationForestDetector, cluster_data: np.ndarray
    ) -> None:
        """Verify StandardScaler transforms features (scaled != raw in general)."""
        raw = cluster_data[0]
        result = trained_detector.detect(raw)
        scaled = np.array(result.details["scaled_features"])
        # After standard-scaling a non-trivial distribution the values should differ.
        assert not np.allclose(raw, scaled), (
            "Scaled features should differ from raw input"
        )

    def test_scores_in_valid_range(
        self, trained_detector: IsolationForestDetector, cluster_data: np.ndarray
    ) -> None:
        """Every score returned by the detector must be in [0, 1]."""
        rng = np.random.default_rng(99)
        # Mix of normal and extreme points.
        test_points = np.vstack(
            [
                cluster_data[:20],
                rng.normal(loc=0, scale=500, size=(10, NUM_FEATURES)),
                np.full((5, NUM_FEATURES), 5000.0),
            ]
        )
        for point in test_points:
            result = trained_detector.detect(point)
            assert 0.0 <= result.score <= 1.0, (
                f"Score {result.score} is out of [0, 1]"
            )

    def test_contamination_parameter(self) -> None:
        """The contamination parameter should be forwarded to the sklearn model."""
        det = IsolationForestDetector(
            contamination=0.25,
            random_state=SEED,
            warm_up_size=WARM_UP_SIZE,
        )
        rng = np.random.default_rng(SEED)
        data = rng.normal(loc=100, scale=10, size=(WARM_UP_SIZE, NUM_FEATURES))
        for row in data:
            det.update(row)
        # After training the internal model should exist with the correct param.
        assert det._model is not None
        assert det._model.contamination == 0.25

    def test_name_property(self) -> None:
        det = IsolationForestDetector()
        assert det.name == "isolation_forest"

    def test_details_contain_raw_score_and_scaled_features(
        self, trained_detector: IsolationForestDetector, cluster_data: np.ndarray
    ) -> None:
        """The details dict should include raw_score and scaled_features."""
        result = trained_detector.detect(cluster_data[0])
        assert "raw_score" in result.details
        assert "scaled_features" in result.details
        assert isinstance(result.details["raw_score"], float)
        assert isinstance(result.details["scaled_features"], list)
        assert len(result.details["scaled_features"]) == NUM_FEATURES
