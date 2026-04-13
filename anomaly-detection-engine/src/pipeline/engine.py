"""Detection engine that orchestrates the full anomaly-detection pipeline."""
from __future__ import annotations

import collections
import threading
from datetime import datetime, timezone

from src.config import Config
from src.detectors.ensemble import EnsembleDecider
from src.detectors.isolation_forest import IsolationForestDetector
from src.detectors.temporal import TemporalPatternDetector
from src.detectors.zscore import ZScoreDetector
from src.models import AnomalyResult, DetectionResult, LogEntry
from src.pipeline.feature_extractor import FeatureExtractor
from src.pipeline.sliding_window import SlidingWindow


class DetectionEngine:
    """End-to-end pipeline: feature extraction -> detection -> ensemble decision.

    Thread-safe statistics are tracked for every processed log entry so the
    dashboard or API can report detection accuracy in real time.

    Args:
        config: A :class:`Config` instance providing all tuning knobs.
    """

    def __init__(self, config: Config) -> None:
        self._config = config

        # Pipeline components
        self._feature_extractor = FeatureExtractor()
        self._sliding_window = SlidingWindow(maxlen=config.window_size)

        # Detectors
        self._zscore = ZScoreDetector(
            threshold=config.zscore_threshold,
            window_size=config.window_size,
        )
        self._iforest = IsolationForestDetector(
            contamination=config.iforest_contamination,
            random_state=config.random_seed,
            warm_up_size=config.warm_up_size,
        )
        self._temporal = TemporalPatternDetector()

        # Ensemble
        self._ensemble = EnsembleDecider(
            weights=config.ensemble_weights,
            threshold=config.ensemble_threshold,
        )

        # Ordered detector list for iteration
        self._detectors = [self._zscore, self._iforest, self._temporal]

        # Stats (guarded by _lock)
        self._lock = threading.Lock()
        self.total_processed: int = 0
        self.anomalies_detected: int = 0
        self.true_positives: int = 0
        self.false_positives: int = 0
        self.false_negatives: int = 0
        self.recent_anomalies: collections.deque[AnomalyResult] = collections.deque(
            maxlen=100,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_log(self, log_entry: LogEntry) -> AnomalyResult:
        """Run a single log entry through the full detection pipeline.

        1. Extract features.
        2. Feed features into the sliding window and all detectors.
        3. Detect with each detector.
        4. Combine via the ensemble decider.
        5. Update stats.

        Returns:
            The final :class:`AnomalyResult`.
        """
        # 1. Feature extraction
        features = self._feature_extractor.extract(log_entry)

        # 2. Update sliding window and detectors
        self._sliding_window.add(features)
        for detector in self._detectors:
            detector.update(features)

        # 3. Detect with each detector
        results: list[DetectionResult] = [
            detector.detect(features) for detector in self._detectors
        ]

        # 4. Ensemble decision
        anomaly_result = self._ensemble.decide(results, log_entry)

        # 5. Thread-safe stats update
        with self._lock:
            self.total_processed += 1

            if anomaly_result.is_anomaly:
                self.anomalies_detected += 1
                self.recent_anomalies.append(anomaly_result)

            # Ground-truth tracking (uses the private _is_anomaly label)
            if log_entry._is_anomaly and anomaly_result.is_anomaly:
                self.true_positives += 1
            elif not log_entry._is_anomaly and anomaly_result.is_anomaly:
                self.false_positives += 1
            elif log_entry._is_anomaly and not anomaly_result.is_anomaly:
                self.false_negatives += 1

        return anomaly_result

    def get_stats(self) -> dict:
        """Return a snapshot of detection statistics.

        The dict contains raw counters as well as derived rates (TPR, FPR,
        detection rate) and a per-detector readiness map.
        """
        with self._lock:
            tp = self.true_positives
            fp = self.false_positives
            fn = self.false_negatives
            total = self.total_processed
            anomalies = self.anomalies_detected

        tp_fn = tp + fn
        actual_negatives = total - tp_fn

        return {
            "total_processed": total,
            "anomalies_detected": anomalies,
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "true_positive_rate": tp / tp_fn if tp_fn > 0 else 0.0,
            "false_positive_rate": fp / actual_negatives if actual_negatives > 0 else 0.0,
            "detection_rate": anomalies / total if total > 0 else 0.0,
            "detectors_ready": {d.name: d.is_ready() for d in self._detectors},
        }

    def get_recent_anomalies(self, limit: int = 50) -> list[dict]:
        """Return the most recent anomalies as serialisable dicts.

        Each dict contains the anomaly timestamp (ISO 8601), confidence,
        is_anomaly flag, detector scores, and a summary of the original
        log entry.
        """
        with self._lock:
            items = list(self.recent_anomalies)

        results: list[dict] = []
        for ar in items[-limit:]:
            results.append(
                {
                    "timestamp": ar.timestamp.isoformat(),
                    "confidence": ar.confidence,
                    "is_anomaly": ar.is_anomaly,
                    "scores": ar.scores,
                    "log_summary": {
                        "ip": ar.log_entry.ip,
                        "method": ar.log_entry.method,
                        "path": ar.log_entry.path,
                        "status_code": ar.log_entry.status_code,
                        "response_time": ar.log_entry.response_time,
                    },
                }
            )

        return results

    def is_warm(self) -> bool:
        """Return ``True`` when every detector reports ready."""
        return all(d.is_ready() for d in self._detectors)
