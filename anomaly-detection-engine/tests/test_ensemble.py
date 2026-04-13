"""Tests for the EnsembleDecider."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.detectors.ensemble import EnsembleDecider
from src.models import DetectionResult, LogEntry


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_log_entry(**overrides) -> LogEntry:
    """Produce a LogEntry with sensible defaults."""
    defaults = {
        "timestamp": datetime.now(timezone.utc),
        "ip": "192.168.1.1",
        "method": "GET",
        "path": "/api/data",
        "status_code": 200,
        "response_time": 150.0,
        "bytes_sent": 5000,
        "user_agent": "Mozilla/5.0",
        "session_duration": 300.0,
        "page_views": 5,
    }
    defaults.update(overrides)
    return LogEntry(**defaults)


def _make_results(
    zscore_score: float = 0.5,
    iforest_score: float = 0.5,
    temporal_score: float = 0.5,
    zscore_ready: bool = True,
    iforest_ready: bool = True,
    temporal_ready: bool = True,
) -> list[DetectionResult]:
    """Build a list of three DetectionResults with configurable scores and readiness."""
    results = []

    zscore_details: dict = {} if zscore_ready else {"ready": False}
    results.append(DetectionResult(score=zscore_score, name="zscore", details=zscore_details))

    iforest_details: dict = {} if iforest_ready else {"ready": False}
    results.append(DetectionResult(score=iforest_score, name="isolation_forest", details=iforest_details))

    temporal_details: dict = {} if temporal_ready else {"ready": False}
    results.append(DetectionResult(score=temporal_score, name="temporal", details=temporal_details))

    return results


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestEnsembleDecider:
    """Unit tests for the EnsembleDecider."""

    def test_all_detectors_agree_anomaly(self):
        """When all detectors report high scores, ensemble flags an anomaly with high confidence."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.7)
        log_entry = _make_log_entry()
        results = _make_results(zscore_score=0.95, iforest_score=0.92, temporal_score=0.90)

        decision = decider.decide(results, log_entry)

        assert decision.is_anomaly is True
        assert decision.confidence > 0.9
        assert "zscore" in decision.scores
        assert "isolation_forest" in decision.scores
        assert "temporal" in decision.scores

    def test_all_detectors_agree_normal(self):
        """When all detectors report low scores, ensemble does NOT flag an anomaly."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.7)
        log_entry = _make_log_entry()
        results = _make_results(zscore_score=0.1, iforest_score=0.15, temporal_score=0.05)

        decision = decider.decide(results, log_entry)

        assert decision.is_anomaly is False
        assert decision.confidence < 0.2

    def test_mixed_signals_threshold_boundary(self):
        """Scores around the threshold boundary are classified correctly."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.7)
        log_entry = _make_log_entry()

        # Weighted sum: 0.7*0.35 + 0.7*0.40 + 0.7*0.25 = 0.7  (exactly at threshold)
        results_at = _make_results(zscore_score=0.7, iforest_score=0.7, temporal_score=0.7)
        decision_at = decider.decide(results_at, log_entry)
        assert decision_at.is_anomaly is True  # >= threshold

        # Weighted sum: 0.65*0.35 + 0.65*0.40 + 0.65*0.25 = 0.65  (below threshold)
        results_below = _make_results(zscore_score=0.65, iforest_score=0.65, temporal_score=0.65)
        decision_below = decider.decide(results_below, log_entry)
        assert decision_below.is_anomaly is False

    def test_weights_affect_outcome(self):
        """High-weight detector (isolation_forest at 0.40) dominates the decision."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.7)
        log_entry = _make_log_entry()

        # iforest high, others low
        results_iforest_high = _make_results(
            zscore_score=0.3, iforest_score=1.0, temporal_score=0.3,
        )
        decision_iforest = decider.decide(results_iforest_high, log_entry)

        # temporal high (lowest weight), others low
        results_temporal_high = _make_results(
            zscore_score=0.3, iforest_score=0.3, temporal_score=1.0,
        )
        decision_temporal = decider.decide(results_temporal_high, log_entry)

        # The iforest-driven confidence should be higher because iforest has weight 0.40
        assert decision_iforest.confidence > decision_temporal.confidence

    def test_confidence_in_valid_range(self):
        """Confidence is always clamped to [0, 1] regardless of input scores."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.5)
        log_entry = _make_log_entry()

        # All zeros
        results_zero = _make_results(zscore_score=0.0, iforest_score=0.0, temporal_score=0.0)
        decision_zero = decider.decide(results_zero, log_entry)
        assert 0.0 <= decision_zero.confidence <= 1.0

        # All ones
        results_max = _make_results(zscore_score=1.0, iforest_score=1.0, temporal_score=1.0)
        decision_max = decider.decide(results_max, log_entry)
        assert 0.0 <= decision_max.confidence <= 1.0

        # Mid-range
        results_mid = _make_results(zscore_score=0.5, iforest_score=0.5, temporal_score=0.5)
        decision_mid = decider.decide(results_mid, log_entry)
        assert 0.0 <= decision_mid.confidence <= 1.0

    def test_threshold_configuration(self):
        """Lower threshold flags more anomalies than higher threshold."""
        log_entry = _make_log_entry()
        results = _make_results(zscore_score=0.5, iforest_score=0.5, temporal_score=0.5)

        low_decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.3)
        high_decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.9)

        low_decision = low_decider.decide(results, log_entry)
        high_decision = high_decider.decide(results, log_entry)

        # Weighted confidence is 0.5 for both; low threshold should flag, high should not
        assert low_decision.is_anomaly is True
        assert high_decision.is_anomaly is False

    def test_handles_not_ready_detectors(self):
        """Only ready detectors contribute; weights are re-normalised."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.5)
        log_entry = _make_log_entry()

        # temporal not ready -> only zscore (0.35) and iforest (0.40) contribute
        results = _make_results(
            zscore_score=0.8,
            iforest_score=0.8,
            temporal_score=0.0,
            temporal_ready=False,
        )
        decision = decider.decide(results, log_entry)

        # Re-normalised weights: zscore=0.35/0.75~0.467, iforest=0.40/0.75~0.533
        # Confidence ~ 0.8 * 0.467 + 0.8 * 0.533 = 0.8
        assert decision.is_anomaly is True
        assert abs(decision.confidence - 0.8) < 0.01
        assert "temporal" not in decision.scores
        assert "zscore" in decision.scores
        assert "isolation_forest" in decision.scores

    def test_no_ready_detectors(self):
        """When no detectors are ready, return is_anomaly=False with confidence=0.0."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.5)
        log_entry = _make_log_entry()

        results = _make_results(
            zscore_score=0.0,
            iforest_score=0.0,
            temporal_score=0.0,
            zscore_ready=False,
            iforest_ready=False,
            temporal_ready=False,
        )
        decision = decider.decide(results, log_entry)

        assert decision.is_anomaly is False
        assert decision.confidence == 0.0
        assert decision.scores == {}

    def test_set_threshold(self):
        """set_threshold dynamically changes the classification boundary."""
        decider = EnsembleDecider(weights=(0.35, 0.40, 0.25), threshold=0.9)
        log_entry = _make_log_entry()
        results = _make_results(zscore_score=0.7, iforest_score=0.7, temporal_score=0.7)

        # With threshold=0.9, confidence 0.7 should NOT flag
        decision_high = decider.decide(results, log_entry)
        assert decision_high.is_anomaly is False

        # Lower the threshold
        decider.set_threshold(0.5)

        # Same data, but now should flag
        decision_low = decider.decide(results, log_entry)
        assert decision_low.is_anomaly is True

    def test_log_entry_attached_to_result(self):
        """The returned AnomalyResult carries the original log entry."""
        decider = EnsembleDecider()
        log_entry = _make_log_entry(ip="10.0.0.99")
        results = _make_results()

        decision = decider.decide(results, log_entry)
        assert decision.log_entry is log_entry
        assert decision.log_entry.ip == "10.0.0.99"

    def test_timestamp_is_set(self):
        """The returned AnomalyResult has a UTC timestamp."""
        decider = EnsembleDecider()
        log_entry = _make_log_entry()
        results = _make_results()

        decision = decider.decide(results, log_entry)
        assert decision.timestamp is not None
        assert decision.timestamp.tzinfo is not None
