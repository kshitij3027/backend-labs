"""Tests for the PartitionRouter."""

import re


class TestSourceRouting:
    """Tests for source-based partition routing."""

    def test_source_routing_determinism(self, source_router):
        """Same source always maps to the same partition."""
        entry = {"source": "web-server-01", "timestamp": "2026-02-28T14:30:00"}
        results = [source_router.route(entry) for _ in range(10)]
        assert len(set(results)) == 1, "Source routing should be deterministic"

    def test_source_routing_distribution(self, source_router):
        """100 different sources distribute across all 3 nodes."""
        partitions = set()
        for i in range(100):
            entry = {"source": f"server-{i}", "timestamp": "2026-02-28T14:30:00"}
            partitions.add(source_router.route(entry))
        assert partitions == {"0", "1", "2"}, (
            f"Expected all 3 partitions to be used, got {partitions}"
        )

    def test_source_routing_format(self, source_router):
        """Result is a string digit '0', '1', or '2' for num_nodes=3."""
        entry = {"source": "app-service", "timestamp": "2026-02-28T14:30:00"}
        result = source_router.route(entry)
        assert result in {"0", "1", "2"}
        assert isinstance(result, str)


class TestTimeRouting:
    """Tests for time-based partition routing."""

    def test_time_routing_format(self, time_router):
        """Result matches pattern YYYYMMDD_HH."""
        entry = {"source": "web-server", "timestamp": "2026-02-28T14:30:00"}
        result = time_router.route(entry)
        assert re.match(r"^\d{8}_\d{2}$", result), (
            f"Expected format YYYYMMDD_HH, got '{result}'"
        )

    def test_time_routing_same_hour(self, time_router):
        """Two timestamps in the same hour go to the same bucket."""
        entry1 = {"source": "server-a", "timestamp": "2026-02-28T14:05:00"}
        entry2 = {"source": "server-b", "timestamp": "2026-02-28T14:55:00"}
        assert time_router.route(entry1) == time_router.route(entry2)

    def test_time_routing_different_hours(self, time_router):
        """Timestamps in different hours go to different buckets."""
        entry1 = {"source": "server-a", "timestamp": "2026-02-28T14:30:00"}
        entry2 = {"source": "server-a", "timestamp": "2026-02-28T15:30:00"}
        assert time_router.route(entry1) != time_router.route(entry2)


class TestHybridRouting:
    """Tests for hybrid partition routing."""

    def test_hybrid_routing_format(self, hybrid_router):
        """Result matches pattern N_YYYYMMDD_HH."""
        entry = {"source": "web-server", "timestamp": "2026-02-28T14:30:00"}
        result = hybrid_router.route(entry)
        assert re.match(r"^\d+_\d{8}_\d{2}$", result), (
            f"Expected format N_YYYYMMDD_HH, got '{result}'"
        )

    def test_hybrid_combines_source_and_time(self, hybrid_router, source_router, time_router):
        """Hybrid result contains both the source partition and time bucket."""
        entry = {"source": "web-server", "timestamp": "2026-02-28T14:30:00"}
        hybrid_result = hybrid_router.route(entry)
        source_result = source_router.route(entry)
        time_result = time_router.route(entry)
        assert hybrid_result == f"{source_result}_{time_result}"


class TestPartitionLookups:
    """Tests for partition lookup methods."""

    def test_get_partition_id_for_source(self, source_router):
        """Returns a consistent single partition for the same source."""
        results = [source_router.get_partition_id_for_source("web-server-01") for _ in range(5)]
        assert len(set(results)) == 1
        assert results[0] in {"0", "1", "2"}

    def test_get_partition_ids_for_time_range(self, time_router):
        """Returns correct number of buckets for a 3-hour range."""
        start = "2026-02-28T10:00:00"
        end = "2026-02-28T12:59:00"
        result = time_router.get_partition_ids_for_time_range(start, end)
        assert len(result) == 3
        assert result == ["20260228_10", "20260228_11", "20260228_12"]

    def test_get_all_partition_ids_for_source_source_strategy(self, source_router):
        """Source strategy returns a list with one element for a given source."""
        result = source_router.get_all_partition_ids_for_source("web-server-01")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] in {"0", "1", "2"}
