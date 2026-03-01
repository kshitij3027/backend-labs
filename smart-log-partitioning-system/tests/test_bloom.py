"""Tests for BloomFilter and bloom filter integration in PartitionManager."""

import pytest

from src.config import PartitionConfig
from src.manager import BloomFilter, PartitionManager


@pytest.fixture
def manager(tmp_path):
    config = PartitionConfig(data_dir=str(tmp_path))
    return PartitionManager(config)


def _make_entry(source="web-server", level="INFO", timestamp="2026-02-28T10:00:00", message="test"):
    return {
        "source": source,
        "level": level,
        "timestamp": timestamp,
        "message": message,
    }


class TestBloomFilter:
    def test_bloom_filter_add_and_check(self):
        """Add items, verify might_contain returns True for added items."""
        bf = BloomFilter()
        bf.add("web-server")
        bf.add("auth-service")
        bf.add("db-service")

        assert bf.might_contain("web-server") is True
        assert bf.might_contain("auth-service") is True
        assert bf.might_contain("db-service") is True

    def test_bloom_filter_definite_negatives(self):
        """Items never added should (very likely with size=1000) return False."""
        bf = BloomFilter(size=1000, num_hashes=3)
        bf.add("web-server")
        bf.add("auth-service")

        # These were never added — with size=1000 and only 2 items,
        # false positive probability is extremely low
        assert bf.might_contain("payment-gateway") is False
        assert bf.might_contain("email-service") is False
        assert bf.might_contain("cache-layer") is False

    def test_bloom_filter_fill_ratio(self):
        """After adding items, fill_ratio > 0."""
        bf = BloomFilter()
        assert bf.fill_ratio == 0.0

        bf.add("web-server")
        bf.add("auth-service")
        bf.add("db-service")

        assert bf.fill_ratio > 0.0
        # With 3 items, 3 hash functions, and size 1000, fill ratio should be small
        assert bf.fill_ratio < 0.1

    def test_bloom_filter_empty(self):
        """Empty bloom filter: fill_ratio is 0, might_contain returns False."""
        bf = BloomFilter()

        assert bf.fill_ratio == 0.0
        assert bf.might_contain("anything") is False
        assert bf.might_contain("") is False
        assert bf.might_contain("web-server") is False


class TestManagerBloomIntegration:
    def test_manager_bloom_filter_integration(self, manager):
        """Store entries with different sources in different partitions,
        verify bloom filter correctly identifies which partitions contain which sources."""
        manager.store("0", _make_entry(source="web-server"))
        manager.store("0", _make_entry(source="web-server"))
        manager.store("1", _make_entry(source="auth-service"))
        manager.store("2", _make_entry(source="db-service"))

        # Partition 0 has web-server
        assert manager.bloom_filters["0"].might_contain("web-server") is True
        assert manager.bloom_filters["0"].might_contain("auth-service") is False

        # Partition 1 has auth-service
        assert manager.bloom_filters["1"].might_contain("auth-service") is True
        assert manager.bloom_filters["1"].might_contain("web-server") is False

        # Partition 2 has db-service
        assert manager.bloom_filters["2"].might_contain("db-service") is True
        assert manager.bloom_filters["2"].might_contain("web-server") is False

    def test_query_with_bloom(self, manager):
        """Store entries, query_with_bloom should return same results as regular query for source filter."""
        manager.store("0", _make_entry(source="web-server", message="web-a"))
        manager.store("0", _make_entry(source="auth-service", message="auth-a"))
        manager.store("1", _make_entry(source="web-server", message="web-b"))
        manager.store("2", _make_entry(source="db-service", message="db-a"))

        all_pids = ["0", "1", "2"]
        filters = {"source": "web-server"}

        bloom_results = manager.query_with_bloom(all_pids, filters)
        regular_results = manager.query(all_pids, filters)

        assert len(bloom_results) == len(regular_results)
        assert bloom_results == regular_results
        assert len(bloom_results) == 2
        assert all(r["source"] == "web-server" for r in bloom_results)

    def test_query_with_bloom_no_source_filter(self, manager):
        """Without source filter, query_with_bloom behaves like regular query."""
        manager.store("0", _make_entry(source="web-server", message="a"))
        manager.store("1", _make_entry(source="auth-service", message="b"))
        manager.store("2", _make_entry(source="db-service", message="c"))

        all_pids = ["0", "1", "2"]

        # No filters at all
        bloom_results = manager.query_with_bloom(all_pids)
        regular_results = manager.query(all_pids)
        assert bloom_results == regular_results
        assert len(bloom_results) == 3

        # Filter by level (not source) — bloom should still return same results
        filters = {"level": "INFO"}
        bloom_results = manager.query_with_bloom(all_pids, filters)
        regular_results = manager.query(all_pids, filters)
        assert bloom_results == regular_results

    def test_bloom_filters_in_stats(self, manager):
        """After storing entries, get_stats() includes bloom_filters info."""
        manager.store("0", _make_entry(source="web-server"))
        manager.store("1", _make_entry(source="auth-service"))

        stats = manager.get_stats()

        assert "bloom_filters" in stats
        assert "0" in stats["bloom_filters"]
        assert "1" in stats["bloom_filters"]
        assert "fill_ratio" in stats["bloom_filters"]["0"]
        assert "fill_ratio" in stats["bloom_filters"]["1"]
        assert stats["bloom_filters"]["0"]["fill_ratio"] > 0
        assert stats["bloom_filters"]["1"]["fill_ratio"] > 0

    def test_bloom_filters_in_stats_empty(self, manager):
        """Empty manager stats should have empty bloom_filters dict."""
        stats = manager.get_stats()
        assert stats["bloom_filters"] == {}

    def test_bloom_filters_rebuilt_on_load(self, tmp_path):
        """Store, create new manager, load_from_disk, verify bloom filters work."""
        config = PartitionConfig(data_dir=str(tmp_path))

        # First manager stores data
        manager1 = PartitionManager(config)
        manager1.store("0", _make_entry(source="web-server"))
        manager1.store("0", _make_entry(source="auth-service"))
        manager1.store("1", _make_entry(source="db-service"))

        # Second manager loads from disk
        manager2 = PartitionManager(config)
        assert len(manager2.bloom_filters) == 0

        manager2.load_from_disk()

        # Bloom filters should be rebuilt
        assert manager2.bloom_filters["0"].might_contain("web-server") is True
        assert manager2.bloom_filters["0"].might_contain("auth-service") is True
        assert manager2.bloom_filters["0"].might_contain("db-service") is False
        assert manager2.bloom_filters["1"].might_contain("db-service") is True
        assert manager2.bloom_filters["1"].might_contain("web-server") is False

        # query_with_bloom should work after load
        results = manager2.query_with_bloom(["0", "1"], {"source": "web-server"})
        assert len(results) == 1
        assert results[0]["source"] == "web-server"
