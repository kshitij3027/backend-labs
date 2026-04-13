"""Tests for the ContextualDetector."""
from __future__ import annotations

import threading

import pytest

from src.advanced.contextual import ContextualDetector
from src.models import LogEntry


class TestContextualDetector:
    """Unit tests for ContextualDetector confidence adjustments."""

    def test_new_ip_gets_higher_multiplier(self, make_log_entry):
        """An IP seen fewer than 3 times should yield a multiplier > 1.0."""
        detector = ContextualDetector()
        entry = make_log_entry(ip="10.0.0.1")

        # IP has never been seen -> count = 0 (< 3)
        multiplier = detector.get_context_adjustment(entry)
        assert multiplier > 1.0, f"Expected > 1.0, got {multiplier}"

        # See it once -> still < 3
        detector.update(entry)
        multiplier = detector.get_context_adjustment(entry)
        assert multiplier > 1.0

    def test_frequent_ip_gets_lower_multiplier(self, make_log_entry):
        """An IP seen more than 100 times should yield a multiplier < 1.0."""
        detector = ContextualDetector()
        entry = make_log_entry(ip="192.168.1.50")

        # Simulate 150 observations for this IP
        for _ in range(150):
            detector.update(entry)

        multiplier = detector.get_context_adjustment(entry)
        assert multiplier < 1.0, f"Expected < 1.0, got {multiplier}"

    def test_maintenance_mode_reduces_multiplier(self, make_log_entry):
        """During maintenance mode, the multiplier should be < 0.7 for a normal IP."""
        detector = ContextualDetector()
        entry = make_log_entry(ip="172.16.0.1")

        # Push the IP into the "normal" range (3..100 observations)
        for _ in range(50):
            detector.update(entry)

        # Without maintenance: normal IP -> factor = 1.0
        no_maint = detector.get_context_adjustment(entry)
        assert no_maint == pytest.approx(1.0)

        # Enable maintenance: factor = 1.0 * 0.6 = 0.6
        detector.set_maintenance_mode(True)
        maint = detector.get_context_adjustment(entry)
        assert maint < 0.7, f"Expected < 0.7 during maintenance, got {maint}"

    def test_multiplier_within_bounds(self, make_log_entry):
        """The multiplier must always be in [0.5, 1.5] regardless of context."""
        detector = ContextualDetector()

        # Case 1: new IP + no maintenance -> 1.3 (in bounds)
        entry_new = make_log_entry(ip="1.2.3.4")
        assert 0.5 <= detector.get_context_adjustment(entry_new) <= 1.5

        # Case 2: new IP + maintenance -> 1.3 * 0.6 = 0.78 (in bounds)
        detector.set_maintenance_mode(True)
        assert 0.5 <= detector.get_context_adjustment(entry_new) <= 1.5

        # Case 3: frequent IP + maintenance -> 0.7 * 0.6 = 0.42 -> clamped to 0.5
        entry_freq = make_log_entry(ip="9.9.9.9")
        for _ in range(200):
            detector.update(entry_freq)
        mult = detector.get_context_adjustment(entry_freq)
        assert mult >= 0.5, f"Expected >= 0.5, got {mult}"
        assert mult <= 1.5

    def test_update_tracks_ip(self, make_log_entry):
        """After calling update, the IP observation count increases."""
        detector = ContextualDetector()
        entry = make_log_entry(ip="10.20.30.40")

        assert detector.get_stats()["unique_ips"] == 0

        detector.update(entry)
        stats = detector.get_stats()
        assert stats["unique_ips"] == 1
        assert stats["total_observations"] == 1

        detector.update(entry)
        assert detector.get_stats()["total_observations"] == 2

    def test_get_stats_returns_expected_keys(self, make_log_entry):
        """get_stats dict must contain unique_ips, maintenance_mode, total_observations."""
        detector = ContextualDetector()
        stats = detector.get_stats()
        assert "unique_ips" in stats
        assert "maintenance_mode" in stats
        assert "total_observations" in stats

    def test_thread_safety(self, make_log_entry):
        """Concurrent updates must not corrupt internal state."""
        detector = ContextualDetector()
        entry = make_log_entry(ip="5.5.5.5")

        def _update_many():
            for _ in range(100):
                detector.update(entry)

        threads = [threading.Thread(target=_update_many) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert detector.get_stats()["total_observations"] == 400
