"""Anomaly detection using rolling Z-score analysis."""

from __future__ import annotations

import numpy as np

from src.models import AnomalyRecord, MetricPoint


class RollingZScoreDetector:
    """Detect anomalies by computing a Z-score against a rolling window of prior values."""

    def __init__(self, threshold: float = 2.5, window_size: int = 100) -> None:
        self.threshold = threshold
        self.window_size = window_size

    def detect(self, data_points: list[MetricPoint]) -> list[AnomalyRecord]:
        """Detect anomalies using rolling Z-score analysis."""
        if len(data_points) < 3:
            return []

        anomalies: list[AnomalyRecord] = []
        values = [p.value for p in data_points]

        for i in range(2, len(data_points)):  # Need at least 2 prior points
            # Window is the preceding points (up to window_size)
            start = max(0, i - self.window_size)
            window = values[start:i]

            mean = float(np.mean(window))
            std = float(np.std(window, ddof=1))  # Use sample std

            if std == 0:
                continue  # Can't compute Z-score with zero std

            z_score = (values[i] - mean) / std

            if abs(z_score) > self.threshold:
                anomalies.append(AnomalyRecord(
                    service=data_points[i].service,
                    metric_name=data_points[i].metric_name,
                    value=data_points[i].value,
                    z_score=round(float(z_score), 4),
                    threshold=self.threshold,
                    mean=round(mean, 4),
                    std=round(std, 4),
                    timestamp=data_points[i].timestamp,
                    is_anomaly=True,
                ))

        return anomalies


def detect_anomalies(data_points: list[MetricPoint], threshold: float = 2.5) -> list[AnomalyRecord]:
    """Convenience wrapper around RollingZScoreDetector."""
    detector = RollingZScoreDetector(threshold=threshold)
    return detector.detect(data_points)
