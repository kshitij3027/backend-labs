"""Ensemble decision maker that combines scores from multiple anomaly detectors."""
from __future__ import annotations

from datetime import datetime, timezone

from src.models import AnomalyResult, DetectionResult, LogEntry


class EnsembleDecider:
    """Combines weighted scores from multiple detectors into a single anomaly decision.

    Each detector is assigned a weight.  When some detectors are not yet ready
    (warm-up phase), their scores are excluded and the remaining weights are
    re-normalised so they still sum to 1.0.

    Args:
        weights: Tuple of floats corresponding to (zscore, isolation_forest, temporal).
        threshold: Minimum weighted confidence to classify as anomaly.
    """

    DETECTOR_WEIGHT_INDEX: dict[str, int] = {
        "zscore": 0,
        "isolation_forest": 1,
        "temporal": 2,
    }

    def __init__(
        self,
        weights: tuple = (0.35, 0.40, 0.25),
        threshold: float = 0.7,
    ) -> None:
        self._weights = weights
        self._threshold = threshold

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def decide(
        self,
        results: list[DetectionResult],
        log_entry: LogEntry,
    ) -> AnomalyResult:
        """Produce a single anomaly verdict from a list of detector results.

        Args:
            results: One :class:`DetectionResult` per detector.
            log_entry: The original log entry being scored.

        Returns:
            An :class:`AnomalyResult` with weighted confidence and per-detector scores.
        """
        # 1. Filter to only "ready" detectors
        ready_results = [
            r for r in results if r.details.get("ready", True)
        ]

        # 2. No ready detectors -> safe default
        if not ready_results:
            return AnomalyResult(
                is_anomaly=False,
                confidence=0.0,
                scores={},
                log_entry=log_entry,
                timestamp=datetime.now(timezone.utc),
            )

        # 3. Collect scores and their associated weights
        ready_scores: list[float] = []
        ready_weights: list[float] = []
        scores_map: dict[str, float] = {}

        for result in ready_results:
            weight_idx = self.DETECTOR_WEIGHT_INDEX.get(result.name)
            if weight_idx is not None and weight_idx < len(self._weights):
                ready_scores.append(result.score)
                ready_weights.append(self._weights[weight_idx])
                scores_map[result.name] = result.score

        # Edge case: all ready results had unknown detector names
        if not ready_scores:
            return AnomalyResult(
                is_anomaly=False,
                confidence=0.0,
                scores={},
                log_entry=log_entry,
                timestamp=datetime.now(timezone.utc),
            )

        # 4. Re-normalise weights so they sum to 1.0
        weight_sum = sum(ready_weights)
        normalised_weights = [w / weight_sum for w in ready_weights]

        # 5. Weighted confidence
        confidence = sum(
            score * weight
            for score, weight in zip(ready_scores, normalised_weights)
        )
        confidence = max(0.0, min(1.0, confidence))

        # 6. Threshold decision
        is_anomaly = confidence >= self._threshold

        return AnomalyResult(
            is_anomaly=is_anomaly,
            confidence=confidence,
            scores=scores_map,
            log_entry=log_entry,
            timestamp=datetime.now(timezone.utc),
        )

    def set_threshold(self, threshold: float) -> None:
        """Change the anomaly confidence threshold dynamically."""
        self._threshold = threshold
