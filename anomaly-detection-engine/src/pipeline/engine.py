"""Detection engine that orchestrates the full anomaly-detection pipeline."""
from __future__ import annotations

import collections
import threading
from datetime import datetime, timezone

from src.advanced.adaptive_threshold import AdaptiveThreshold
from src.advanced.contextual import ContextualDetector
from src.advanced.false_positive import FalsePositiveManager
from src.advanced.memory_efficient import PatternStore
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

        # Adaptive threshold
        self._adaptive_threshold = AdaptiveThreshold(
            initial_threshold=config.ensemble_threshold,
        )

        # Contextual detection and false positive management
        self._contextual = ContextualDetector()
        self._fp_manager = FalsePositiveManager()

        # Memory-efficient pattern storage (HLL + CMS)
        self._pattern_store = PatternStore()

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

        # 5. Contextual adjustment (post-ensemble multiplier)
        self._contextual.update(log_entry)
        adjustment = self._contextual.get_context_adjustment(log_entry)
        adjusted_confidence = max(0.0, min(1.0, anomaly_result.confidence * adjustment))
        adjusted_is_anomaly = adjusted_confidence >= self._ensemble._threshold

        anomaly_result = AnomalyResult(
            is_anomaly=adjusted_is_anomaly,
            confidence=adjusted_confidence,
            scores=anomaly_result.scores,
            log_entry=log_entry,
            timestamp=anomaly_result.timestamp,
        )

        # 6. Update adaptive threshold and feed back to ensemble
        #    Only adjust once detectors are warmed up to avoid premature drift.
        if self.is_warm():
            self._adaptive_threshold.update(
                was_flagged=anomaly_result.is_anomaly,
                was_true_anomaly=log_entry._is_anomaly,
            )
            self._ensemble.set_threshold(self._adaptive_threshold.get_threshold())

        # 7. False positive tracking
        if anomaly_result.is_anomaly:
            self._fp_manager.add_anomaly({
                "anomaly_id": str(id(anomaly_result)),
                "ip": log_entry.ip,
                "status_code": log_entry.status_code,
                "method": log_entry.method,
                "path": log_entry.path,
                "confidence": anomaly_result.confidence,
                "anomaly_type": getattr(log_entry, "_anomaly_type", "unknown"),
            })

        # 8. Record pattern in memory-efficient store
        self._pattern_store.add_pattern(
            log_entry.ip, log_entry.user_agent, log_entry.path,
        )

        # 9. Thread-safe stats update
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

    def feedback(self, anomaly_id: str, confirmed: bool) -> None:
        """Forward operator feedback to the adaptive threshold.

        Args:
            anomaly_id: Identifier of the anomaly being reviewed.
            confirmed: ``True`` if the operator confirms a real anomaly.
        """
        self._adaptive_threshold.operator_feedback(anomaly_id, confirmed)
        self._ensemble.set_threshold(self._adaptive_threshold.get_threshold())

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
            "adaptive_threshold": self._adaptive_threshold.get_stats(),
            "contextual": self._contextual.get_stats(),
            "false_positive_manager": self._fp_manager.get_stats(),
            "memory_efficient": self._pattern_store.get_stats(),
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

    def get_anomaly_groups(self) -> list[dict]:
        """Return current anomaly groups from the false positive manager.

        Each group is serialised to a dict with group_id, count,
        common_features, first_seen, last_seen, and the raw anomalies.
        """
        groups = self._fp_manager.group_anomalies()
        return [
            {
                "group_id": g.group_id,
                "count": g.count,
                "common_features": g.common_features,
                "first_seen": g.first_seen.isoformat(),
                "last_seen": g.last_seen.isoformat(),
                "anomalies": g.anomalies,
            }
            for g in groups
        ]

    def is_warm(self) -> bool:
        """Return ``True`` when every detector reports ready."""
        return all(d.is_ready() for d in self._detectors)
