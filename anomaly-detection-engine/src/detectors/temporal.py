"""Temporal pattern based anomaly detector.

Detects anomalies by comparing response times against per-hour and per-weekday
baselines. A request that is normal at 2pm might be anomalous at 3am if the
historical response-time distribution for that hour is very different.
"""
from __future__ import annotations

import threading
from collections import defaultdict

import numpy as np

from src.detectors.base import BaseDetector
from src.models import DetectionResult


class TemporalPatternDetector(BaseDetector):
    """Detects anomalies based on temporal patterns in response times.

    Maintains per-hour (0-23) and per-weekday (0-6) baselines of observed
    response times.  During detection the current response time is compared
    against the baseline for the corresponding hour and weekday; the larger
    deviation (in standard-deviation units) drives the anomaly score.

    Thread-safety is ensured via an internal :class:`threading.Lock`.
    """

    def __init__(self, min_bucket_size: int = 5) -> None:
        self._hourly_baselines: dict[int, list[float]] = defaultdict(list)
        self._weekday_baselines: dict[int, list[float]] = defaultdict(list)
        self._hourly_volumes: dict[int, list[int]] = defaultdict(list)
        self._lock = threading.Lock()
        self._min_bucket_size = min_bucket_size
        self._total_updates: int = 0

    # ------------------------------------------------------------------
    # BaseDetector interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:  # noqa: D401
        """Detector identifier."""
        return "temporal"

    def update(self, features: np.ndarray) -> None:
        """Record an observation's response time in the appropriate buckets.

        Args:
            features: 1-D numpy array where index 0 = hour, 1 = weekday,
                      3 = response_time.
        """
        hour = int(features[0])
        weekday = int(features[1])
        response_time = float(features[3])

        with self._lock:
            self._hourly_baselines[hour].append(response_time)
            self._weekday_baselines[weekday].append(response_time)
            self._total_updates += 1

    def detect(self, features: np.ndarray) -> DetectionResult:
        """Score a feature vector against the learned temporal baselines.

        Returns a :class:`DetectionResult` whose score represents how unusual
        the response time is for the current hour and weekday.  A deviation
        of 3 standard deviations maps to a score of 1.0.
        """
        hour = int(features[0])
        weekday = int(features[1])
        response_time = float(features[3])

        with self._lock:
            hourly_bucket = list(self._hourly_baselines.get(hour, []))
            weekday_bucket = list(self._weekday_baselines.get(weekday, []))

        # Check if the specific buckets are ready
        if not self._bucket_ready(hourly_bucket, self._min_bucket_size) or \
           not self._bucket_ready(weekday_bucket, self._min_bucket_size):
            return DetectionResult(
                score=0.0,
                name=self.name,
                details={"ready": False},
            )

        # Compute hourly deviation
        hourly_arr = np.array(hourly_bucket, dtype=np.float64)
        hourly_mean = float(np.mean(hourly_arr))
        hourly_std = float(np.std(hourly_arr))
        hourly_deviation = abs(response_time - hourly_mean) / max(hourly_std, 1e-6)

        # Compute weekday deviation
        weekday_arr = np.array(weekday_bucket, dtype=np.float64)
        weekday_mean = float(np.mean(weekday_arr))
        weekday_std = float(np.std(weekday_arr))
        weekday_deviation = abs(response_time - weekday_mean) / max(weekday_std, 1e-6)

        # Combined deviation: take the worst case
        combined_deviation = max(hourly_deviation, weekday_deviation)

        # Normalize to [0, 1]: deviation of 3 std = full confidence
        confidence = min(combined_deviation / 3.0, 1.0)

        return DetectionResult(
            score=confidence,
            name=self.name,
            details={
                "hourly_deviation": hourly_deviation,
                "weekday_deviation": weekday_deviation,
                "hourly_mean": hourly_mean,
                "weekday_mean": weekday_mean,
                "current_hour": hour,
                "current_weekday": weekday,
            },
        )

    def is_ready(self) -> bool:
        """Return True once enough observations have been recorded.

        The detector is considered ready when the total number of updates
        is at least ``2 * min_bucket_size``, indicating sufficient data
        exists in general (individual bucket readiness is checked per-detect).
        """
        with self._lock:
            return self._total_updates >= self._min_bucket_size * 2

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _bucket_ready(bucket_list: list, min_size: int) -> bool:
        """Return True if *bucket_list* has at least *min_size* entries."""
        return len(bucket_list) >= min_size
