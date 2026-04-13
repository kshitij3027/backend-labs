"""Abstract base class for anomaly detectors."""
from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np

from src.models import DetectionResult


class BaseDetector(ABC):
    """Interface that every anomaly detector must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return a short identifier string for this detector."""

    @abstractmethod
    def update(self, features: np.ndarray) -> None:
        """Feed a feature vector so the detector can learn / warm up.

        Args:
            features: 1-D numpy array of shape (num_features,).
        """

    @abstractmethod
    def detect(self, features: np.ndarray) -> DetectionResult:
        """Score a single feature vector and return a DetectionResult.

        Args:
            features: 1-D numpy array of shape (num_features,).

        Returns:
            DetectionResult with score in [0, 1], detector name, and details dict.
        """

    @abstractmethod
    def is_ready(self) -> bool:
        """Return True when the detector has enough data to produce meaningful scores."""
