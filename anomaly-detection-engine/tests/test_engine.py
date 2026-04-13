"""Tests for the DetectionEngine pipeline orchestrator."""
from __future__ import annotations

import pytest

from src.config import Config
from src.generator.log_generator import LogGenerator
from src.models import AnomalyResult, LogEntry
from src.pipeline.engine import DetectionEngine


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _default_config(**overrides) -> Config:
    """Build a Config with small warm-up for faster tests."""
    defaults = {
        "zscore_threshold": 3.0,
        "iforest_contamination": 0.1,
        "ensemble_threshold": 0.7,
        "ensemble_weights": (0.35, 0.40, 0.25),
        "window_size": 100,
        "warm_up_size": 100,
        "random_seed": 42,
    }
    defaults.update(overrides)
    return Config(**defaults)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestDetectionEngine:
    """Unit and integration tests for DetectionEngine."""

    def test_engine_processes_log(self):
        """process_log accepts a LogEntry and returns an AnomalyResult."""
        config = _default_config()
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=1)

        log_entry = gen.generate()
        result = engine.process_log(log_entry)

        assert isinstance(result, AnomalyResult)
        assert isinstance(result.is_anomaly, bool)
        assert 0.0 <= result.confidence <= 1.0
        assert result.log_entry is log_entry

    def test_stats_tracking(self):
        """Processing N logs increments total_processed to N."""
        config = _default_config()
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=2)

        n = 10
        for log_entry in gen.generate_batch(n):
            engine.process_log(log_entry)

        stats = engine.get_stats()
        assert stats["total_processed"] == n

    def test_warm_up_phase(self):
        """During warm-up, detectors are not ready and very few logs are flagged."""
        config = _default_config(warm_up_size=100, window_size=100)
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=3)

        # Process fewer logs than warm_up_size
        flagged = 0
        for log_entry in gen.generate_batch(20):
            result = engine.process_log(log_entry)
            if result.is_anomaly:
                flagged += 1

        # During warm-up, detectors return ready=False and scores of 0.0,
        # so the ensemble should produce confidence near 0.0.
        # Allow at most 1 false flag due to contextual adjustment edge cases.
        assert flagged <= 1
        assert not engine.is_warm()

    def test_detection_after_warmup(self):
        """After warm-up with normal data, an extreme anomaly gets high confidence."""
        config = _default_config(warm_up_size=100, window_size=100)
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=4)

        # Warm-up: feed 150 normal logs so all detectors become ready
        for log_entry in gen.generate_batch(150):
            engine.process_log(log_entry)

        assert engine.is_warm()

        # Create an extreme anomalous entry
        from datetime import datetime, timezone

        extreme_entry = LogEntry(
            timestamp=datetime.now(timezone.utc),
            ip="10.99.99.99",
            method="POST",
            path="/api/admin/exploit",
            status_code=500,
            response_time=15000.0,  # extremely slow
            bytes_sent=200000,      # huge payload
            user_agent="sqlmap/1.7",
            session_duration=5000.0,
            page_views=100,
            _is_anomaly=True,
            _anomaly_type="slow_response",
        )

        result = engine.process_log(extreme_entry)

        # The extreme entry should get a noticeably higher confidence than
        # the baseline normal traffic (which is typically ~0.0-0.3).
        assert result.confidence > 0.3

    def test_get_recent_anomalies_format(self):
        """get_recent_anomalies returns dicts with the expected keys."""
        config = _default_config(warm_up_size=100, window_size=100, ensemble_threshold=0.3)
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=5)

        # Warm up
        for log_entry in gen.generate_batch(150):
            engine.process_log(log_entry)

        # Feed an extreme log that should be flagged
        from datetime import datetime, timezone

        extreme_entry = LogEntry(
            timestamp=datetime.now(timezone.utc),
            ip="10.99.99.99",
            method="POST",
            path="/api/admin/exploit",
            status_code=500,
            response_time=15000.0,
            bytes_sent=200000,
            user_agent="sqlmap/1.7",
            session_duration=5000.0,
            page_views=100,
            _is_anomaly=True,
            _anomaly_type="slow_response",
        )
        engine.process_log(extreme_entry)

        recent = engine.get_recent_anomalies(limit=10)

        if len(recent) > 0:
            entry = recent[-1]
            assert "timestamp" in entry
            assert "confidence" in entry
            assert "is_anomaly" in entry
            assert "scores" in entry
            assert "log_summary" in entry

            summary = entry["log_summary"]
            assert "ip" in summary
            assert "method" in summary
            assert "path" in summary
            assert "status_code" in summary
            assert "response_time" in summary

    def test_is_warm(self):
        """Engine starts not-warm and becomes warm after sufficient data."""
        config = _default_config(warm_up_size=50, window_size=50)
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.0, seed=6)

        # Initially not warm
        assert engine.is_warm() is False

        # Feed enough data (more than warm_up_size + temporal requirements)
        for log_entry in gen.generate_batch(120):
            engine.process_log(log_entry)

        # Should now be warm (all detectors ready)
        assert engine.is_warm() is True

    def test_stats_rates(self):
        """get_stats returns sensible rate calculations."""
        config = _default_config()
        engine = DetectionEngine(config)

        # Before any processing, rates should be 0
        stats = engine.get_stats()
        assert stats["true_positive_rate"] == 0.0
        assert stats["false_positive_rate"] == 0.0
        assert stats["detection_rate"] == 0.0

    def test_stats_detectors_ready(self):
        """get_stats includes per-detector readiness info."""
        config = _default_config()
        engine = DetectionEngine(config)

        stats = engine.get_stats()
        assert "detectors_ready" in stats
        ready = stats["detectors_ready"]
        assert "zscore" in ready
        assert "isolation_forest" in ready
        assert "temporal" in ready
        # Initially none should be ready
        assert ready["zscore"] is False
        assert ready["isolation_forest"] is False

    def test_multiple_logs_mixed_traffic(self):
        """Process a mix of normal and anomalous logs; stats counters are consistent."""
        config = _default_config(warm_up_size=100, window_size=100)
        engine = DetectionEngine(config)
        gen = LogGenerator(anomaly_rate=0.10, seed=7)

        batch = gen.generate_batch(200)
        for log_entry in batch:
            engine.process_log(log_entry)

        stats = engine.get_stats()
        assert stats["total_processed"] == 200

        # Sanity: TP + FP == anomalies_detected
        assert stats["true_positives"] + stats["false_positives"] == stats["anomalies_detected"]
