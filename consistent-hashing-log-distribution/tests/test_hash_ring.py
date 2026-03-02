"""Comprehensive tests for the HashRing consistent hashing implementation."""

import random
import string
import threading
import time
from collections import Counter

import pytest

from src.hash_ring import HashRing


class TestEmptyRing:
    """Tests for an empty hash ring."""

    def test_get_node_returns_none(self, empty_ring):
        """get_node should return None when ring is empty."""
        assert empty_ring.get_node("any-key") is None

    def test_get_nodes_returns_empty_list(self, empty_ring):
        """get_nodes should return an empty list when ring is empty."""
        assert empty_ring.get_nodes("any-key", 3) == []

    def test_ring_metrics_empty(self, empty_ring):
        """get_ring_metrics should return zero counts for empty ring."""
        metrics = empty_ring.get_ring_metrics()
        assert metrics["total_vnodes"] == 0
        assert metrics["nodes"] == {}


class TestSingleNode:
    """Tests for a ring with a single node."""

    def test_all_keys_map_to_single_node(self, single_node_ring):
        """All keys should map to the only node in the ring."""
        for i in range(100):
            key = f"test-key-{i}"
            assert single_node_ring.get_node(key) == "node1"

    def test_get_nodes_single_node(self, single_node_ring):
        """get_nodes should return only the single node regardless of count."""
        result = single_node_ring.get_nodes("some-key", 3)
        assert result == ["node1"]


class TestConsistentMapping:
    """Tests for consistent key-to-node mapping."""

    def test_same_key_same_node(self, three_node_ring):
        """The same key should always map to the same node."""
        key = "consistent-test-key"
        first_result = three_node_ring.get_node(key)
        for _ in range(100):
            assert three_node_ring.get_node(key) == first_result

    def test_different_keys_deterministic(self, three_node_ring):
        """Different keys should deterministically map to nodes."""
        mappings = {}
        for i in range(50):
            key = f"key-{i}"
            mappings[key] = three_node_ring.get_node(key)

        # Verify all mappings are consistent on re-check
        for key, expected_node in mappings.items():
            assert three_node_ring.get_node(key) == expected_node


class TestAddNode:
    """Tests for adding nodes to the ring."""

    def test_add_node_returns_metadata(self, empty_ring):
        """add_node should return metadata dict with expected keys."""
        result = empty_ring.add_node("node1")
        assert isinstance(result, dict)
        assert result["node_id"] == "node1"
        assert result["vnodes_added"] == 150
        assert "affected_ranges" in result

    def test_add_node_makes_node_available(self, empty_ring):
        """After adding a node, keys should map to it."""
        empty_ring.add_node("new-node")
        assert empty_ring.get_node("test-key") == "new-node"

    def test_add_multiple_nodes(self, empty_ring):
        """Adding multiple nodes should distribute keys across them."""
        for i in range(5):
            empty_ring.add_node(f"node{i}")

        nodes_seen = set()
        for i in range(100):
            node = empty_ring.get_node(f"key-{i}")
            nodes_seen.add(node)

        # With 5 nodes and 100 keys, we should see multiple nodes used
        assert len(nodes_seen) > 1


class TestRemoveNode:
    """Tests for removing nodes from the ring."""

    def test_remove_node_returns_metadata(self, three_node_ring):
        """remove_node should return metadata dict with expected keys."""
        result = three_node_ring.remove_node("node2")
        assert isinstance(result, dict)
        assert result["node_id"] == "node2"
        assert result["vnodes_removed"] == 150
        assert "affected_ranges" in result

    def test_keys_never_map_to_removed_node(self, three_node_ring):
        """After removing a node, no keys should map to it."""
        three_node_ring.remove_node("node2")
        for i in range(1000):
            key = f"test-key-{i}"
            assert three_node_ring.get_node(key) != "node2"

    def test_remove_all_nodes(self, three_node_ring):
        """Removing all nodes should result in empty ring behavior."""
        three_node_ring.remove_node("node1")
        three_node_ring.remove_node("node2")
        three_node_ring.remove_node("node3")
        assert three_node_ring.get_node("key") is None


class TestDistributionBalance:
    """Tests for key distribution evenness."""

    def test_distribution_balance_three_nodes(self, three_node_ring):
        """With 3 nodes and 10K keys, each node should get ~33% (within +-5%)."""
        counter = Counter()
        num_keys = 10000

        for i in range(num_keys):
            key = f"distribution-test-key-{i}"
            node = three_node_ring.get_node(key)
            counter[node] += 1

        expected_fraction = 1.0 / 3.0
        tolerance = 0.05

        for node_id, count in counter.items():
            actual_fraction = count / num_keys
            assert abs(actual_fraction - expected_fraction) < tolerance, (
                f"Node {node_id} got {actual_fraction:.3f} "
                f"(expected {expected_fraction:.3f} +/- {tolerance})"
            )


class TestMinimalMovement:
    """Tests for minimal key redistribution on topology changes."""

    def test_add_node_minimal_movement(self):
        """Adding a 4th node to a 3-node ring should move ~25% of keys (+-5%)."""
        ring = HashRing(nodes=["node1", "node2", "node3"])
        num_keys = 10000

        # Record initial mappings
        initial_mappings = {}
        for i in range(num_keys):
            key = f"movement-key-{i}"
            initial_mappings[key] = ring.get_node(key)

        # Add a 4th node
        ring.add_node("node4")

        # Count how many keys moved
        moved = 0
        for key, original_node in initial_mappings.items():
            new_node = ring.get_node(key)
            if new_node != original_node:
                moved += 1

        move_fraction = moved / num_keys
        expected = 0.25  # ~1/4 of keys should move
        tolerance = 0.05

        assert abs(move_fraction - expected) < tolerance, (
            f"Key movement was {move_fraction:.3f} "
            f"(expected ~{expected} +/- {tolerance})"
        )

    def test_remove_node_minimal_movement(self):
        """Removing one node from a 4-node ring should move ~25% of keys."""
        ring = HashRing(nodes=["node1", "node2", "node3", "node4"])
        num_keys = 10000

        # Record initial mappings
        initial_mappings = {}
        for i in range(num_keys):
            key = f"removal-key-{i}"
            initial_mappings[key] = ring.get_node(key)

        # Remove one node
        ring.remove_node("node4")

        # Count how many keys moved (keys that were on node4 must move)
        moved = 0
        for key, original_node in initial_mappings.items():
            new_node = ring.get_node(key)
            if new_node != original_node:
                moved += 1

        move_fraction = moved / num_keys
        expected = 0.25  # ~1/4 of keys should move
        tolerance = 0.05

        assert abs(move_fraction - expected) < tolerance, (
            f"Key movement was {move_fraction:.3f} "
            f"(expected ~{expected} +/- {tolerance})"
        )


class TestReplication:
    """Tests for get_nodes replication support."""

    def test_get_nodes_returns_distinct_physical_nodes(self, three_node_ring):
        """get_nodes should return distinct physical nodes."""
        result = three_node_ring.get_nodes("replication-key", 3)
        assert len(result) == 3
        assert len(set(result)) == 3  # all distinct

    def test_get_nodes_fewer_than_requested(self):
        """When requesting more nodes than exist, return all available."""
        ring = HashRing(nodes=["node1", "node2"])
        result = ring.get_nodes("some-key", 5)
        assert len(result) == 2
        assert set(result) == {"node1", "node2"}

    def test_get_nodes_order_is_clockwise(self, three_node_ring):
        """get_nodes should return nodes in clockwise order from the key."""
        result = three_node_ring.get_nodes("order-test-key", 3)
        # The first node should be the same as get_node
        primary = three_node_ring.get_node("order-test-key")
        assert result[0] == primary

    def test_get_nodes_single_returns_one(self, three_node_ring):
        """Requesting 1 node should return the primary node."""
        result = three_node_ring.get_nodes("single-key", 1)
        assert len(result) == 1
        assert result[0] == three_node_ring.get_node("single-key")


class TestLookupPerformance:
    """Performance benchmarks for hash ring operations."""

    def test_lookup_performance(self):
        """100K lookups should complete in under 2 seconds (50K+ lookups/sec)."""
        ring = HashRing(nodes=[f"node{i}" for i in range(10)])
        num_lookups = 100000

        # Pre-generate keys to exclude key generation from timing
        keys = [f"perf-key-{i}" for i in range(num_lookups)]

        start = time.time()
        for key in keys:
            ring.get_node(key)
        elapsed = time.time() - start

        assert elapsed < 2.0, (
            f"100K lookups took {elapsed:.2f}s "
            f"({num_lookups / elapsed:.0f} lookups/sec)"
        )


class TestThreadSafety:
    """Tests for concurrent access safety."""

    def test_thread_safety_no_crashes(self):
        """Concurrent adds, removes, and lookups should not crash."""
        ring = HashRing(nodes=["node1", "node2", "node3"])
        errors = []

        def add_remove_worker(worker_id):
            """Worker that adds and removes nodes."""
            try:
                node_name = f"thread-node-{worker_id}"
                for _ in range(50):
                    ring.add_node(node_name)
                    # Do some lookups while node exists
                    for j in range(10):
                        ring.get_node(f"key-{worker_id}-{j}")
                    ring.remove_node(node_name)
            except Exception as e:
                errors.append(e)

        def lookup_worker(worker_id):
            """Worker that continuously performs lookups."""
            try:
                for i in range(500):
                    ring.get_node(f"lookup-{worker_id}-{i}")
                    ring.get_nodes(f"repl-{worker_id}-{i}", 2)
            except Exception as e:
                errors.append(e)

        threads = []

        # Spawn 5 add/remove workers
        for i in range(5):
            t = threading.Thread(target=add_remove_worker, args=(i,))
            threads.append(t)

        # Spawn 5 lookup workers
        for i in range(5):
            t = threading.Thread(target=lookup_worker, args=(i,))
            threads.append(t)

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all to complete
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0, f"Thread safety errors: {errors}"

    def test_concurrent_metrics(self):
        """get_ring_metrics should be safe under concurrent access."""
        ring = HashRing(nodes=["node1", "node2", "node3"])
        errors = []

        def metrics_worker():
            try:
                for _ in range(100):
                    metrics = ring.get_ring_metrics()
                    assert "total_vnodes" in metrics
                    assert "nodes" in metrics
            except Exception as e:
                errors.append(e)

        def mutate_worker():
            try:
                for _ in range(50):
                    ring.add_node("temp-node")
                    ring.remove_node("temp-node")
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=metrics_worker))
            threads.append(threading.Thread(target=mutate_worker))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert len(errors) == 0, f"Concurrent metrics errors: {errors}"


class TestAdjustVnodes:
    """Tests for dynamic virtual node adjustment."""

    def test_adjust_vnodes_increase(self, three_node_ring):
        """Increasing vnodes for a node should reflect in the result."""
        result = three_node_ring.adjust_vnodes("node1", 300)
        assert result["node_id"] == "node1"
        assert result["new_count"] == 300
        assert result["old_count"] == 150
        assert result["vnodes_added"] == 150
        assert result["vnodes_removed"] == 0

    def test_adjust_vnodes_decrease(self, three_node_ring):
        """Decreasing vnodes for a node should reflect in the result."""
        result = three_node_ring.adjust_vnodes("node1", 50)
        assert result["node_id"] == "node1"
        assert result["old_count"] == 150
        assert result["new_count"] == 50
        assert result["vnodes_removed"] == 100
        assert result["vnodes_added"] == 0

    def test_adjust_vnodes_nonexistent_node(self, three_node_ring):
        """Adjusting vnodes for a node not in the ring should raise ValueError."""
        with pytest.raises(ValueError, match="not in ring"):
            three_node_ring.adjust_vnodes("nonexistent-node", 100)

    def test_adjust_vnodes_load_changes(self, three_node_ring):
        """After doubling node1's vnodes, node1 should have a higher load_percent."""
        metrics_before = three_node_ring.get_ring_metrics()
        load_before = metrics_before["nodes"]["node1"]["load_percent"]

        three_node_ring.adjust_vnodes("node1", 300)

        metrics_after = three_node_ring.get_ring_metrics()
        load_after = metrics_after["nodes"]["node1"]["load_percent"]

        assert load_after > load_before, (
            f"Expected node1 load to increase after doubling vnodes: "
            f"before={load_before:.2f}%, after={load_after:.2f}%"
        )


class TestRingMetrics:
    """Tests for ring distribution metrics."""

    def test_metrics_returns_per_node_data(self, three_node_ring):
        """Metrics should contain data for each node."""
        metrics = three_node_ring.get_ring_metrics()
        assert "nodes" in metrics
        assert "node1" in metrics["nodes"]
        assert "node2" in metrics["nodes"]
        assert "node3" in metrics["nodes"]

    def test_metrics_vnode_counts(self, three_node_ring):
        """Each node should have 150 vnodes."""
        metrics = three_node_ring.get_ring_metrics()
        for node_id, node_metrics in metrics["nodes"].items():
            assert node_metrics["vnode_count"] == 150

    def test_metrics_total_vnodes(self, three_node_ring):
        """Total vnodes should equal nodes * vnodes_per_node."""
        metrics = three_node_ring.get_ring_metrics()
        assert metrics["total_vnodes"] == 450  # 3 * 150

    def test_metrics_load_percentages_sum_to_100(self, three_node_ring):
        """Load percentages should sum to approximately 100%."""
        metrics = three_node_ring.get_ring_metrics()
        total_load = sum(
            node_metrics["load_percent"]
            for node_metrics in metrics["nodes"].values()
        )
        assert abs(total_load - 100.0) < 0.1, (
            f"Load percentages sum to {total_load:.2f}%, expected ~100%"
        )

    def test_metrics_load_roughly_balanced(self, three_node_ring):
        """With 3 nodes, each should have roughly 33% load."""
        metrics = three_node_ring.get_ring_metrics()
        for node_id, node_metrics in metrics["nodes"].items():
            load = node_metrics["load_percent"]
            assert 20.0 < load < 46.0, (
                f"Node {node_id} has {load:.1f}% load, "
                f"expected roughly 33%"
            )

    def test_metrics_has_load_percent(self, single_node_ring):
        """Single node should have ~100% load."""
        metrics = single_node_ring.get_ring_metrics()
        node_metrics = metrics["nodes"]["node1"]
        assert abs(node_metrics["load_percent"] - 100.0) < 0.1
