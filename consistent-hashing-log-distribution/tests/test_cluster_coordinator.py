"""Tests for the cluster coordinator module."""

import pytest
from src.cluster_coordinator import ClusterCoordinator
from src.config import ClusterConfig, NodeConfig


def _make_log(source: str, message: str, level: str = "INFO", ts: str = "2026-01-01T00:00:00Z"):
    """Helper to build a log entry dict."""
    return {
        "source": source,
        "message": message,
        "level": level,
        "timestamp": ts,
    }


class TestClusterCoordinatorInit:

    def test_init_with_config(self, cluster_config):
        """Creating with a ClusterConfig with 3 nodes populates the cluster."""
        coord = ClusterCoordinator(cluster_config)
        node_ids = coord.get_node_ids()
        assert len(node_ids) == 3
        assert set(node_ids) == {"node1", "node2", "node3"}

    def test_init_without_config(self):
        """Creating with no config yields an empty cluster."""
        coord = ClusterCoordinator()
        assert coord.get_node_ids() == []


class TestStoreLog:

    def test_store_log(self, coordinator):
        """Storing a single log returns a result with expected keys."""
        log = _make_log("web-server", "request received")
        result = coordinator.store_log(log)

        assert "node_id" in result
        assert result["node_id"] in coordinator.get_node_ids()
        assert "log_key" in result
        assert "entry" in result
        assert result["entry"]["source"] == "web-server"
        assert result["entry"]["message"] == "request received"
        assert "stored_at" in result["entry"]
        assert "node_id" in result["entry"]

    def test_store_log_no_nodes(self):
        """Storing to an empty cluster raises ValueError."""
        coord = ClusterCoordinator()
        with pytest.raises(ValueError, match="No nodes available"):
            coord.store_log(_make_log("app", "hello"))

    def test_store_logs_batch(self, coordinator):
        """Storing 100 logs in batch returns valid results for each."""
        logs = [
            _make_log(f"source-{i % 5}", f"message-{i}", ts=f"2026-01-01T00:{i:02d}:00Z")
            for i in range(100)
        ]
        results = coordinator.store_logs(logs)

        assert len(results) == 100
        valid_nodes = set(coordinator.get_node_ids())
        for r in results:
            assert r["node_id"] in valid_nodes
            assert "log_key" in r
            assert "entry" in r


class TestDistribution:

    def test_distribution_balance(self, coordinator):
        """10K logs with varied sources are distributed within +-5% of 33.3% per node."""
        logs = [
            _make_log(
                f"source-{i % 20}",
                f"event-{i}",
                level=["INFO", "WARN", "ERROR"][i % 3],
                ts=f"2026-01-{(i % 28) + 1:02d}T{i % 24:02d}:00:00Z",
            )
            for i in range(10_000)
        ]
        coordinator.store_logs(logs)

        metrics = coordinator.get_cluster_metrics()
        assert metrics["total_logs"] == 10_000

        expected_pct = 100.0 / 3.0  # ~33.33%
        for node_id, stats in metrics["nodes"].items():
            pct = stats["log_percent"]
            assert abs(pct - expected_pct) < 5.0, (
                f"Node {node_id} has {pct:.2f}% of logs, "
                f"expected ~{expected_pct:.2f}% (within +-5%)"
            )


class TestRebalancing:

    def _store_n_logs(self, coordinator, n=1000):
        """Store n diverse logs and return total count."""
        logs = [
            _make_log(
                f"src-{i % 10}",
                f"msg-{i}",
                ts=f"2026-01-01T{i % 24:02d}:{i % 60:02d}:00Z",
            )
            for i in range(n)
        ]
        coordinator.store_logs(logs)
        return n

    def test_add_node_rebalance(self, coordinator):
        """Adding a 4th node rebalances ~25% of logs with zero data loss."""
        total = self._store_n_logs(coordinator, 1000)

        # Count logs before
        metrics_before = coordinator.get_cluster_metrics()
        assert metrics_before["total_logs"] == total

        # Add a 4th node
        result = coordinator.add_node("node4")

        assert result["node_id"] == "node4"
        assert "ring_update" in result
        assert "logs_migrated" in result
        assert "migration_time_ms" in result

        # The new node should have received some logs
        metrics_after = coordinator.get_cluster_metrics()
        new_node_count = metrics_after["nodes"]["node4"]["log_count"]
        assert new_node_count > 0, "New node should have received some logs"

        # Approximately 25% of logs should have migrated (within +-5%)
        migrated_pct = result["logs_migrated"] / total * 100
        assert abs(migrated_pct - 25.0) < 5.0, (
            f"Expected ~25% migration, got {migrated_pct:.1f}%"
        )

        # Zero data loss
        assert metrics_after["total_logs"] == total

    def test_remove_node_rebalance(self, coordinator):
        """Removing a node redistributes all its logs with zero data loss."""
        total = self._store_n_logs(coordinator, 1000)

        metrics_before = coordinator.get_cluster_metrics()
        removed_node_logs = metrics_before["nodes"]["node2"]["log_count"]

        result = coordinator.remove_node("node2")

        assert result["node_id"] == "node2"
        assert result["logs_migrated"] == removed_node_logs
        assert "migration_time_ms" in result

        # Zero data loss
        metrics_after = coordinator.get_cluster_metrics()
        assert metrics_after["total_logs"] == total
        assert metrics_after["node_count"] == 2
        assert "node2" not in metrics_after["nodes"]

    def test_zero_data_loss_add_remove(self, coordinator):
        """Store 5000 logs, add a node, remove a different node: total unchanged."""
        total = self._store_n_logs(coordinator, 5000)

        coordinator.add_node("node4")
        metrics_mid = coordinator.get_cluster_metrics()
        assert metrics_mid["total_logs"] == total

        coordinator.remove_node("node1")
        metrics_final = coordinator.get_cluster_metrics()
        assert metrics_final["total_logs"] == total
        assert metrics_final["node_count"] == 3
        assert set(coordinator.get_node_ids()) == {"node2", "node3", "node4"}

    def test_add_then_remove_same_node(self, coordinator):
        """Adding then removing the same node keeps the system stable."""
        total = self._store_n_logs(coordinator, 500)

        coordinator.add_node("node-temp")
        assert "node-temp" in coordinator.get_node_ids()

        coordinator.remove_node("node-temp")
        assert "node-temp" not in coordinator.get_node_ids()

        metrics = coordinator.get_cluster_metrics()
        assert metrics["total_logs"] == total
        assert metrics["node_count"] == 3


class TestMetrics:

    def test_ring_update_timing(self, coordinator):
        """Adding a node reports migration_time_ms and it is fast for small datasets."""
        logs = [_make_log(f"s-{i}", f"m-{i}") for i in range(100)]
        coordinator.store_logs(logs)

        result = coordinator.add_node("node-fast")
        assert "migration_time_ms" in result
        assert isinstance(result["migration_time_ms"], float)
        # Should be well under 50ms for 100 logs
        assert result["migration_time_ms"] < 50.0

    def test_get_cluster_metrics(self, coordinator):
        """Cluster metrics contain all expected top-level keys."""
        logs = [_make_log(f"s-{i}", f"m-{i}") for i in range(50)]
        coordinator.store_logs(logs)

        metrics = coordinator.get_cluster_metrics()

        assert "total_logs" in metrics
        assert metrics["total_logs"] == 50
        assert "node_count" in metrics
        assert metrics["node_count"] == 3
        assert "nodes" in metrics
        assert len(metrics["nodes"]) == 3
        assert "ring_metrics" in metrics
        assert "balance_variance" in metrics
        assert isinstance(metrics["balance_variance"], float)

        # Each node entry should have log_count and log_percent
        for node_id, stats in metrics["nodes"].items():
            assert "log_count" in stats
            assert "log_percent" in stats
