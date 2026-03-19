"""Tests for metrics collector."""
import threading
import pytest
from src.monitoring.metrics import MetricsCollector


class TestMetricsCollector:
    def test_initial_snapshot(self):
        m = MetricsCollector()
        snap = m.snapshot()
        assert snap["total_consumed"] == 0
        assert snap["total_errors"] == 0
        assert snap["uptime_seconds"] >= 0
        assert snap["throughput"] == []
        assert snap["per_consumer"] == {}
        assert snap["per_partition"] == {}
        assert snap["rebalance_events"] == []
        assert snap["lag"] == {}
        assert snap["scaling_events"] == []

    def test_record_consumed(self):
        m = MetricsCollector()
        m.record_consumed("c-0", partition=0, count=5)
        m.record_consumed("c-0", partition=1, count=3)
        m.record_consumed("c-1", partition=2, count=2)
        snap = m.snapshot()
        assert snap["total_consumed"] == 10
        assert snap["per_partition"] == {0: 5, 1: 3, 2: 2}
        assert snap["per_consumer"]["c-0"]["consumed"] == 8
        assert snap["per_consumer"]["c-1"]["consumed"] == 2
        assert 0 in snap["per_consumer"]["c-0"]["partitions"]
        assert 1 in snap["per_consumer"]["c-0"]["partitions"]

    def test_record_error(self):
        m = MetricsCollector()
        m.record_error("c-0", count=3)
        snap = m.snapshot()
        assert snap["total_errors"] == 3
        assert snap["per_consumer"]["c-0"]["errors"] == 3

    def test_record_rebalance(self):
        m = MetricsCollector()
        m.record_rebalance("assign", [0, 1, 2], "c-0")
        snap = m.snapshot()
        assert len(snap["rebalance_events"]) == 1
        assert snap["rebalance_events"][0]["type"] == "assign"
        assert snap["rebalance_events"][0]["partitions"] == [0, 1, 2]

    def test_update_lag(self):
        m = MetricsCollector()
        m.update_lag(0, 100)
        m.update_lag(1, 50)
        snap = m.snapshot()
        assert snap["lag"] == {0: 100, 1: 50}

    def test_thread_safety(self):
        """Verify no crashes under concurrent access."""
        m = MetricsCollector()
        errors = []

        def writer(cid):
            try:
                for i in range(100):
                    m.record_consumed(cid, partition=i % 6)
                    m.record_throughput(float(i))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(f"c-{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        snap = m.snapshot()
        assert snap["total_consumed"] == 500

    def test_snapshot_returns_copy(self):
        """Modifying snapshot should not affect internal state."""
        m = MetricsCollector()
        m.record_consumed("c-0", 0)
        snap1 = m.snapshot()
        snap1["total_consumed"] = 999
        snap2 = m.snapshot()
        assert snap2["total_consumed"] == 1
