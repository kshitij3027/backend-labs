"""Tests for the FalsePositiveManager."""
from __future__ import annotations

import time

import pytest

from src.advanced.false_positive import AnomalyGroup, FalsePositiveManager


class TestFalsePositiveManager:
    """Unit tests for anomaly grouping, feedback, and accuracy tracking."""

    def test_anomalies_with_same_subnet_grouped(self):
        """Two anomalies from the same /24 subnet within the time window are grouped."""
        mgr = FalsePositiveManager(time_window=60.0)

        now = time.time()
        mgr.add_anomaly({"ip": "192.168.1.10", "status_code": 500, "_added_ts": now})
        mgr.add_anomaly({"ip": "192.168.1.20", "status_code": 403, "_added_ts": now + 5})

        groups = mgr.group_anomalies()

        # Both IPs share the 192.168.1 subnet, so they should land in one group.
        assert len(groups) == 1
        assert groups[0].count == 2
        assert isinstance(groups[0], AnomalyGroup)

    def test_anomalies_far_apart_not_grouped(self):
        """Anomalies separated by more than the time window are not grouped together."""
        mgr = FalsePositiveManager(time_window=60.0)

        now = time.time()
        mgr.add_anomaly({"ip": "192.168.1.10", "status_code": 500, "_added_ts": now})
        mgr.add_anomaly({"ip": "192.168.1.20", "status_code": 403, "_added_ts": now + 600})

        groups = mgr.group_anomalies()

        # Even though subnet matches, time gap exceeds 60 s -> separate groups.
        assert len(groups) == 2

    def test_feedback_updates_accuracy(self):
        """Recording confirmed/dismissed feedback updates historical accuracy counts."""
        mgr = FalsePositiveManager()

        mgr.record_feedback("a1", confirmed=True)
        mgr.record_feedback("a2", confirmed=False)
        mgr.record_feedback("a3", confirmed=True)

        accuracy = mgr.get_historical_accuracy()

        # All were "unknown" type since no matching anomaly was stored
        assert "unknown" in accuracy
        assert accuracy["unknown"]["confirmed"] == 2
        assert accuracy["unknown"]["dismissed"] == 1

    def test_add_anomaly_prunes_old(self):
        """Anomalies older than 5 minutes are pruned when a new one is added."""
        mgr = FalsePositiveManager()

        old_ts = time.time() - 400  # older than 300 s (5 min)
        mgr.add_anomaly({"ip": "10.0.0.1", "_added_ts": old_ts})

        # Adding a fresh anomaly triggers pruning
        mgr.add_anomaly({"ip": "10.0.0.2", "_added_ts": time.time()})

        groups = mgr.group_anomalies()

        # The old entry should have been pruned; only the fresh one remains.
        total_anomalies = sum(g.count for g in groups)
        assert total_anomalies == 1

    def test_get_stats_returns_expected_keys(self):
        """get_stats must contain total_feedback, confirmed_count, dismissed_count, groups_count."""
        mgr = FalsePositiveManager()
        stats = mgr.get_stats()

        assert "total_feedback" in stats
        assert "confirmed_count" in stats
        assert "dismissed_count" in stats
        assert "groups_count" in stats

    def test_stats_reflect_feedback(self):
        """Stats counters accurately reflect recorded feedback."""
        mgr = FalsePositiveManager()

        mgr.record_feedback("x1", confirmed=True)
        mgr.record_feedback("x2", confirmed=True)
        mgr.record_feedback("x3", confirmed=False)

        stats = mgr.get_stats()
        assert stats["total_feedback"] == 3
        assert stats["confirmed_count"] == 2
        assert stats["dismissed_count"] == 1

    def test_group_same_status_code(self):
        """Anomalies with the same status code within the time window are grouped."""
        mgr = FalsePositiveManager(time_window=60.0)

        now = time.time()
        mgr.add_anomaly({"ip": "10.0.0.1", "status_code": 500, "_added_ts": now})
        mgr.add_anomaly({"ip": "172.16.0.1", "status_code": 500, "_added_ts": now + 2})

        groups = mgr.group_anomalies()

        # Different subnets but same status code -> grouped
        assert len(groups) == 1
        assert groups[0].count == 2

    def test_empty_grouping(self):
        """Grouping with no anomalies returns an empty list."""
        mgr = FalsePositiveManager()
        groups = mgr.group_anomalies()
        assert groups == []
