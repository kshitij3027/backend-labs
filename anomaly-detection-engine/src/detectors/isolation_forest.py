"""Isolation Forest based anomaly detector with StandardScaler normalization."""
from __future__ import annotations

import threading

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.detectors.base import BaseDetector
from src.models import DetectionResult


class IsolationForestDetector(BaseDetector):
    """Detects anomalies using scikit-learn's Isolation Forest.

    During a warm-up phase the detector accumulates feature vectors.  Once
    *warm_up_size* vectors have been collected, a :class:`StandardScaler` is
    fitted on the collected data and an :class:`IsolationForest` model is
    trained on the scaled features.

    After training, incoming feature vectors are scaled and scored.  The raw
    ``score_samples`` value (negative for anomalies, near zero for normal
    points) is converted to a ``[0, 1]`` confidence where higher values
    indicate a stronger anomaly signal.

    Thread-safety is provided by an internal :class:`threading.Lock` that
    guards all model access.
    """

    def __init__(
        self,
        contamination: float = 0.1,
        random_state: int = 42,
        n_estimators: int = 100,
        warm_up_size: int = 100,
    ) -> None:
        self._contamination = contamination
        self._random_state = random_state
        self._n_estimators = n_estimators
        self._warm_up_size = warm_up_size

        self._scaler = StandardScaler()
        self._model: IsolationForest | None = None
        self._training_data: list[np.ndarray] = []
        self._is_trained: bool = False

        self._score_min: float = 0.0
        self._score_max: float = 0.0

        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # BaseDetector interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:  # noqa: D401
        """Detector identifier."""
        return "isolation_forest"

    def update(self, features: np.ndarray) -> None:
        """Accumulate a feature vector during the warm-up phase.

        Once *warm_up_size* vectors have been collected the model is trained
        automatically.
        """
        with self._lock:
            if self._is_trained:
                return

            if len(self._training_data) < self._warm_up_size:
                self._training_data.append(features.copy())

            if len(self._training_data) == self._warm_up_size:
                self._train()

    def detect(self, features: np.ndarray) -> DetectionResult:
        """Score *features* against the trained Isolation Forest.

        Returns a :class:`DetectionResult` with ``score`` in ``[0, 1]``.  If
        the model has not been trained yet the score is ``0.0`` and the
        details dict contains ``{"ready": False}``.
        """
        if not self.is_ready():
            return DetectionResult(
                score=0.0,
                name=self.name,
                details={"ready": False},
            )

        with self._lock:
            scaled = self._scaler.transform(features.reshape(1, -1))
            raw_score = float(self._model.score_samples(scaled)[0])

        # Update running min / max (outside lock for brevity; slight race is
        # acceptable because the normalisation is only approximate).
        self._score_min = min(self._score_min, raw_score)
        self._score_max = max(self._score_max, raw_score)

        # Normalise to [0, 1].  Lower raw scores are more anomalous so we
        # invert the mapping.
        if self._score_max == self._score_min:
            confidence = 0.5
        else:
            confidence = 1.0 - (raw_score - self._score_min) / (
                self._score_max - self._score_min
            )

        confidence = max(0.0, min(1.0, confidence))

        return DetectionResult(
            score=confidence,
            name=self.name,
            details={
                "raw_score": raw_score,
                "scaled_features": scaled.flatten().tolist(),
            },
        )

    def is_ready(self) -> bool:
        """Return ``True`` once the model has been trained on warm-up data."""
        return self._is_trained

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _train(self) -> None:
        """Fit the scaler and Isolation Forest on the accumulated warm-up data.

        This method is called **while the lock is held** by :meth:`update`.
        """
        data = np.stack(self._training_data)

        # 1. Fit the scaler and transform training data.
        self._scaler.fit(data)
        scaled_data = self._scaler.transform(data)

        # 2. Create and fit the Isolation Forest.
        self._model = IsolationForest(
            contamination=self._contamination,
            random_state=self._random_state,
            n_estimators=self._n_estimators,
        )
        self._model.fit(scaled_data)

        # 3. Establish initial score range from the training data.
        train_scores = self._model.score_samples(scaled_data)
        self._score_min = float(np.min(train_scores))
        self._score_max = float(np.max(train_scores))

        # 4. Mark as trained and release the training data.
        self._is_trained = True
        self._training_data = []
