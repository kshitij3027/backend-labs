"""Z-score based anomaly detector."""
from __future__ import annotations

import numpy as np

from src.detectors.base import BaseDetector
from src.models import DetectionResult
from src.pipeline.feature_extractor import FeatureExtractor
from src.pipeline.sliding_window import SlidingWindow


class ZScoreDetector(BaseDetector):
    """Detects anomalies by computing per-feature z-scores against a sliding window.

    A feature vector is scored by finding the maximum absolute z-score across
    all features and converting it to a [0, 1] confidence via the configured
    threshold.

    Thread-safety is inherited from :class:`SlidingWindow`.
    """

    def __init__(
        self,
        threshold: float = 3.0,
        window_size: int = 100,
        min_ready_size: int = 30,
    ) -> None:
        self._threshold = threshold
        self._min_ready_size = min_ready_size
        self._window = SlidingWindow(maxlen=window_size)

    # ------------------------------------------------------------------
    # BaseDetector interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:  # noqa: D401
        """Detector identifier."""
        return "zscore"

    def update(self, features: np.ndarray) -> None:
        """Add a feature vector to the internal sliding window."""
        self._window.add(features)

    def detect(self, features: np.ndarray) -> DetectionResult:
        """Compute the z-score anomaly confidence for *features*.

        Returns a DetectionResult whose score is 0.0 when the detector has
        not yet accumulated enough baseline data.
        """
        if not self.is_ready():
            return DetectionResult(
                score=0.0,
                name=self.name,
                details={"ready": False},
            )

        mean, std = self._window.get_stats()

        z_scores = np.abs(features - mean) / (std + 1e-10)
        max_z = float(np.max(z_scores))
        max_idx = int(np.argmax(z_scores))

        confidence = min(max_z / self._threshold, 1.0)

        feature_names = FeatureExtractor.FEATURE_NAMES
        triggered_name = (
            feature_names[max_idx]
            if max_idx < len(feature_names)
            else f"feature_{max_idx}"
        )

        return DetectionResult(
            score=confidence,
            name=self.name,
            details={
                "max_z_score": max_z,
                "triggered_feature_idx": max_idx,
                "triggered_feature_name": triggered_name,
                "per_feature_z": [float(z) for z in z_scores],
            },
        )

    def is_ready(self) -> bool:
        """Return True once the sliding window has at least *min_ready_size* entries."""
        return self._window.is_ready(self._min_ready_size)
