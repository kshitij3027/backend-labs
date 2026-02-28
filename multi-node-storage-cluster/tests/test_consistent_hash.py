"""Tests for consistent hash ring."""

import pytest

from src.consistent_hash import HashRing


class TestHashRing:
    """Unit tests for the HashRing class."""

    def test_get_node_returns_node(self):
        """Add 3 nodes, verify get_node returns one of them."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        result = ring.get_node("some-file.txt")
        assert result in nodes

    def test_get_nodes_returns_correct_count(self):
        """Verify get_nodes(key, 2) returns exactly 2 distinct nodes."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        result = ring.get_nodes("some-file.txt", 2)
        assert len(result) == 2

    def test_get_nodes_no_duplicates(self):
        """Verify all returned nodes are unique."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        result = ring.get_nodes("some-file.txt", 3)
        assert len(result) == len(set(result))

    def test_consistent_mapping(self):
        """Same key always maps to the same node."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        key = "test-key-for-consistency"
        first_result = ring.get_node(key)
        for _ in range(100):
            assert ring.get_node(key) == first_result

    def test_distribution(self):
        """1000 keys distributed across 3 nodes, each gets at least 20%."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        counts = {n: 0 for n in nodes}
        for i in range(1000):
            node = ring.get_node(f"key-{i}")
            counts[node] += 1
        for node, count in counts.items():
            assert count >= 200, (
                f"{node} got only {count}/1000 keys ({count / 10:.1f}%), "
                f"expected at least 20%. Distribution: {counts}"
            )

    def test_add_node_stability(self):
        """Adding a 4th node doesn't remap all existing keys (at least 50% stable)."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)

        # Record mapping before adding a node
        keys = [f"stability-key-{i}" for i in range(1000)]
        before = {k: ring.get_node(k) for k in keys}

        # Add a 4th node
        ring.add_node("node4")

        # Check how many keys stayed on the same node
        stable = sum(1 for k in keys if ring.get_node(k) == before[k])
        assert stable >= 500, (
            f"Only {stable}/1000 keys remained stable after adding node4 "
            f"({stable / 10:.1f}%), expected at least 50%"
        )

    def test_remove_node(self):
        """Remove a node, verify it no longer appears in results."""
        nodes = ["node1", "node2", "node3"]
        ring = HashRing(nodes)
        ring.remove_node("node2")

        # Check many keys to confirm node2 never appears
        for i in range(500):
            result = ring.get_node(f"remove-test-{i}")
            assert result != "node2", f"Removed node2 still returned for key remove-test-{i}"
            result_list = ring.get_nodes(f"remove-test-{i}", 2)
            assert "node2" not in result_list, (
                f"Removed node2 still in get_nodes result for key remove-test-{i}"
            )

    def test_empty_ring(self):
        """get_node and get_nodes return None/[] on empty ring."""
        ring = HashRing([])
        assert ring.get_node("anything") is None
        assert ring.get_nodes("anything", 3) == []

    def test_single_node(self):
        """get_nodes(key, 3) returns only 1 node when ring has 1 node."""
        ring = HashRing(["only-node"])
        result = ring.get_nodes("some-key", 3)
        assert len(result) == 1
        assert result[0] == "only-node"
