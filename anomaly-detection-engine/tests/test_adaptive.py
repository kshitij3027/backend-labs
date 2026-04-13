"""Tests for the AdaptiveThreshold module."""
from __future__ import annotations

import pytest

from src.advanced.adaptive_threshold import AdaptiveThreshold


class TestAdaptiveThreshold:
    """Suite for AdaptiveThreshold behaviour."""

    def test_threshold_starts_at_initial(self) -> None:
        """Threshold should equal the initial value before any updates."""
        at = AdaptiveThreshold(initial_threshold=0.6)
        assert at.get_threshold() == pytest.approx(0.6)

    def test_high_fpr_increases_threshold(self) -> None:
        """Many false positives should push the threshold up."""
        at = AdaptiveThreshold(
            initial_threshold=0.5,
            alpha=0.3,
            target_fpr=0.05,
            adjustment_step=0.02,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        initial = at.get_threshold()

        # Feed many false positives (flagged=True, true_anomaly=False)
        for _ in range(50):
            at.update(was_flagged=True, was_true_anomaly=False)

        assert at.get_threshold() > initial

    def test_low_fpr_decreases_threshold(self) -> None:
        """Consistently low FPR (true positives) should lower the threshold."""
        at = AdaptiveThreshold(
            initial_threshold=0.7,
            alpha=0.3,
            target_fpr=0.05,
            adjustment_step=0.02,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        initial = at.get_threshold()

        # Feed many true positives (flagged=True, true_anomaly=True) -> FPR = 0
        for _ in range(50):
            at.update(was_flagged=True, was_true_anomaly=True)

        assert at.get_threshold() < initial

    def test_threshold_stays_within_bounds(self) -> None:
        """Extreme cases must not exceed min/max bounds."""
        at = AdaptiveThreshold(
            initial_threshold=0.5,
            alpha=0.5,
            target_fpr=0.05,
            adjustment_step=0.1,
            min_threshold=0.3,
            max_threshold=0.95,
        )

        # Push threshold down aggressively
        for _ in range(200):
            at.update(was_flagged=True, was_true_anomaly=True)
        assert at.get_threshold() >= 0.3

        # Reset and push threshold up aggressively
        at2 = AdaptiveThreshold(
            initial_threshold=0.5,
            alpha=0.5,
            target_fpr=0.05,
            adjustment_step=0.1,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        for _ in range(200):
            at2.update(was_flagged=True, was_true_anomaly=False)
        assert at2.get_threshold() <= 0.95

    def test_load_factor_adjusts_threshold(self) -> None:
        """Setting load_factor > 1 should increase the effective threshold."""
        at = AdaptiveThreshold(initial_threshold=0.5)
        base = at.get_threshold()

        at.set_load_factor(1.5)
        assert at.get_threshold() > base

    def test_operator_feedback_confirmed(self) -> None:
        """Confirmed true-positive feedback should decrease threshold."""
        at = AdaptiveThreshold(
            initial_threshold=0.6,
            adjustment_step=0.02,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        before = at.get_threshold()
        at.operator_feedback("anomaly-1", confirmed=True)
        assert at.get_threshold() < before

    def test_operator_feedback_dismissed(self) -> None:
        """Dismissed (false positive) feedback should increase threshold."""
        at = AdaptiveThreshold(
            initial_threshold=0.6,
            adjustment_step=0.02,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        before = at.get_threshold()
        at.operator_feedback("anomaly-2", confirmed=False)
        assert at.get_threshold() > before

    def test_ewma_smoothing(self) -> None:
        """A single false positive should not cause a drastic jump."""
        at = AdaptiveThreshold(
            initial_threshold=0.5,
            alpha=0.1,
            target_fpr=0.05,
            adjustment_step=0.02,
            min_threshold=0.3,
            max_threshold=0.95,
        )
        before = at.get_threshold()
        at.update(was_flagged=True, was_true_anomaly=False)
        after = at.get_threshold()

        # Change should be at most one adjustment_step
        assert abs(after - before) <= 0.02 + 1e-9

    def test_get_stats_returns_expected_keys(self) -> None:
        """get_stats() must contain all documented keys."""
        at = AdaptiveThreshold()
        stats = at.get_stats()

        expected_keys = {
            "current_threshold",
            "ewma_fpr",
            "load_factor",
            "total_updates",
            "feedback_count",
        }
        assert set(stats.keys()) == expected_keys

    def test_stats_update_after_operations(self) -> None:
        """Stats counters should reflect updates and feedback."""
        at = AdaptiveThreshold(initial_threshold=0.5)

        at.update(was_flagged=True, was_true_anomaly=False)
        at.update(was_flagged=False, was_true_anomaly=False)
        at.operator_feedback("a1", confirmed=True)

        stats = at.get_stats()
        assert stats["total_updates"] == 2
        assert stats["feedback_count"] == 1
