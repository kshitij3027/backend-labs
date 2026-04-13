"""Feature extraction from log entries for anomaly detection."""
from __future__ import annotations

import numpy as np

from src.models import LogEntry


class FeatureExtractor:
    """Extracts a fixed-size numeric feature vector from a LogEntry."""

    FEATURE_NAMES: list[str] = [
        "hour",
        "weekday",
        "minute",
        "response_time",
        "status_code",
        "bytes_sent",
        "session_duration",
        "page_views",
        "user_agent_length",
    ]
    NUM_FEATURES: int = 9

    def extract(self, log_entry: LogEntry) -> np.ndarray:
        """Extract a feature vector from a single log entry.

        Returns a numpy array of shape (9,) with dtype float64.
        """
        features = np.array(
            [
                float(log_entry.timestamp.hour),
                float(log_entry.timestamp.weekday()),
                float(log_entry.timestamp.minute),
                log_entry.response_time,
                float(log_entry.status_code),
                float(log_entry.bytes_sent),
                log_entry.session_duration,
                float(log_entry.page_views),
                float(len(log_entry.user_agent)),
            ],
            dtype=np.float64,
        )
        return features
