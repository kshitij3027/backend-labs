import time
import pytest
from unittest.mock import MagicMock
from src.merkle.tree import MerkleTree
from src.coordinator.scanner import AntiEntropyScanner
from src.coordinator.repair import RepairWorker
from src.coordinator.strategies import Strategy
from src.metrics import ConsistencyMetrics


def make_mock_client(node_id, data_dict):
    """Create a mock NodeClient with consistent data."""
    client = MagicMock()
    client.node_id = node_id

    tree = MerkleTree({k: v for k, v in data_dict.items()})
    client.get_merkle_root.return_value = tree.root_hash
    client.get_merkle_leaves.return_value = tree.get_leaf_hashes()

    def get_data(key):
        if key in data_dict:
            return {"key": key, "value": data_dict[key], "version": 1, "timestamp": time.time()}
        return None
    client.get_data.side_effect = get_data
    client.put_data.return_value = True
    return client


class TestPerformance:
    def test_merkle_tree_build_1000_entries(self, sample_data_large):
        """Merkle tree build < 50ms for 1000 entries."""
        start = time.time()
        tree = MerkleTree(sample_data_large)
        duration = (time.time() - start) * 1000
        assert duration < 50, f"Tree build took {duration:.1f}ms, expected < 50ms"
        assert tree.root_hash is not None

    def test_detection_speed_per_pair(self):
        """Detection < 100ms per pair."""
        data_a = {f"key-{i}": f"val-{i}" for i in range(100)}
        data_b = dict(data_a)
        data_b["key-50"] = "different"

        client_a = make_mock_client("a", data_a)
        client_b = make_mock_client("b", data_b)
        scanner = AntiEntropyScanner([client_a, client_b])

        start = time.time()
        scanner.run_scan()
        duration = (time.time() - start) * 1000
        assert duration < 100, f"Detection took {duration:.1f}ms, expected < 100ms"

    def test_single_repair_speed(self):
        """Single repair < 500ms."""
        client = MagicMock()
        client.node_id = "test"
        client.put_data.return_value = True

        from src.coordinator.scanner import RepairTask
        task = RepairTask(key="k", target_node=client, value="v", version=1, timestamp=1.0)
        worker = RepairWorker()
        worker.add_tasks([task])

        start = time.time()
        worker.execute_repairs()
        duration = (time.time() - start) * 1000
        assert duration < 500, f"Repair took {duration:.1f}ms, expected < 500ms"

    def test_full_scan_3_nodes(self):
        """Full scan < 5s for 3 nodes."""
        data = {f"key-{i}": f"val-{i}" for i in range(100)}
        clients = [make_mock_client(f"node-{c}", data) for c in "abc"]
        scanner = AntiEntropyScanner(clients)

        start = time.time()
        scanner.run_scan()
        duration = time.time() - start
        assert duration < 5.0, f"Full scan took {duration:.2f}s, expected < 5s"


class TestIntegration:
    def test_full_scan_repair_cycle(self):
        """Full scan-repair cycle: detect inconsistency, repair it."""
        data_good = {"k1": "v1", "k2": "v2"}
        data_bad = {"k1": "v1", "k2": "WRONG"}

        client_a = make_mock_client("a", data_good)
        client_b = make_mock_client("b", data_good)
        client_c = make_mock_client("c", data_bad)

        scanner = AntiEntropyScanner([client_a, client_b, client_c])
        tasks = scanner.run_scan()
        assert len(tasks) > 0, "Should detect inconsistency"

        worker = RepairWorker()
        worker.add_tasks(tasks)
        completed, failed = worker.execute_repairs()
        assert completed > 0
        assert failed == 0

    def test_identical_trees_no_repair(self):
        """Identical data across all nodes produces no repair tasks."""
        data = {"k1": "v1", "k2": "v2", "k3": "v3"}
        clients = [make_mock_client(f"node-{c}", data) for c in "abc"]
        scanner = AntiEntropyScanner(clients)
        tasks = scanner.run_scan()
        assert len(tasks) == 0

    def test_metrics_increase_over_repairs(self):
        """Metrics should increase as repairs happen."""
        metrics = ConsistencyMetrics()
        assert metrics.repairs_completed == 0

        metrics.record_repair(3, 0, 0.1)
        assert metrics.repairs_completed == 3

        metrics.record_repair(2, 1, 0.2)
        assert metrics.repairs_completed == 5
        assert metrics.repairs_failed == 1

    def test_different_trees_produce_different_hashes(self):
        tree_a = MerkleTree({"k1": "v1"})
        tree_b = MerkleTree({"k1": "v2"})
        assert tree_a.root_hash != tree_b.root_hash

    def test_identical_trees_produce_same_hash(self):
        data = {"k1": "v1", "k2": "v2"}
        tree_a = MerkleTree(data)
        tree_b = MerkleTree(data)
        assert tree_a.root_hash == tree_b.root_hash
