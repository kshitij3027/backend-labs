"""Tests for the QueryOptimizer class."""

import hashlib

import pytest

from src.config import PartitionConfig
from src.manager import PartitionManager
from src.optimizer import QueryOptimizer
from src.router import PartitionRouter


# --- Helper -------------------------------------------------------------------

def _make_entry(source="web_server", level="INFO", timestamp="2026-02-28T10:00:00", message="test"):
    return {
        "source": source,
        "level": level,
        "timestamp": timestamp,
        "message": message,
    }


def _source_partition(source: str, num_nodes: int = 3) -> str:
    """Compute the source partition the same way the router does."""
    return str(int(hashlib.md5(source.encode()).hexdigest(), 16) % num_nodes)


def _populate_source_manager(router: PartitionRouter, manager: PartitionManager):
    """Store entries across all 3 source partitions.

    Uses sources that hash to distinct partitions with num_nodes=3:
        api_gateway -> 0
        web_server  -> 1
        database    -> 2
    """
    sources = ["api_gateway", "web_server", "database"]
    # Verify our assumptions about the hash mapping
    for s in sources:
        pid = router.route({"source": s, "timestamp": "2026-02-28T10:00:00"})
        assert pid == _source_partition(s), f"{s} expected partition {_source_partition(s)}, got {pid}"

    for source in sources:
        for i in range(5):
            entry = _make_entry(source=source, message=f"{source}-msg-{i}")
            pid = router.route(entry)
            manager.store(pid, entry)

    return sources


# --- Source strategy tests ----------------------------------------------------

class TestSourcePruning:
    def test_source_pruning_to_single_partition(self, source_optimizer, source_router, source_manager):
        """Querying by source with source strategy scans exactly 1 of 3 partitions."""
        _populate_source_manager(source_router, source_manager)

        result = source_optimizer.optimize({"source": "web_server"})

        assert result["partitions_scanned"] == 1
        assert result["total_partitions"] == 3
        assert result["pruned"] == 2
        assert result["improvement_factor"] == 3.0
        # The single partition ID should be the one web_server hashes to
        expected_pid = _source_partition("web_server")
        assert result["partition_ids"] == [expected_pid]

    def test_no_filter_scans_all_partitions(self, source_optimizer, source_router, source_manager):
        """Query with no filters scans every partition."""
        _populate_source_manager(source_router, source_manager)

        result = source_optimizer.optimize({})

        assert result["partitions_scanned"] == 3
        assert result["total_partitions"] == 3
        assert result["pruned"] == 0
        assert result["improvement_factor"] == 1.0


# --- Time strategy tests -----------------------------------------------------

class TestTimePruning:
    def test_time_pruning(self, tmp_path):
        """Querying a 2-hour window when data spans 24 hours scans only 2 buckets."""
        config = PartitionConfig(strategy="time", time_bucket_hours=1, data_dir=str(tmp_path))
        router = PartitionRouter(config)
        manager = PartitionManager(config)
        optimizer = QueryOptimizer(router, manager)

        # Populate data across 24 hours
        for hour in range(24):
            entry = _make_entry(
                source="web_server",
                timestamp=f"2026-02-28T{hour:02d}:30:00",
                message=f"hour-{hour}",
            )
            pid = router.route(entry)
            manager.store(pid, entry)

        assert len(manager.get_all_partition_ids()) == 24

        result = optimizer.optimize({
            "time_range": {
                "start": "2026-02-28T10:00:00",
                "end": "2026-02-28T11:59:00",
            }
        })

        assert result["partitions_scanned"] == 2
        assert result["total_partitions"] == 24
        assert result["pruned"] == 22
        assert result["improvement_factor"] == 12.0
        assert "20260228_10" in result["partition_ids"]
        assert "20260228_11" in result["partition_ids"]


# --- Hybrid strategy tests ----------------------------------------------------

class TestHybridPruning:
    @pytest.fixture
    def hybrid_setup(self, tmp_path):
        """Hybrid optimizer populated with data across sources and hours."""
        config = PartitionConfig(
            strategy="hybrid", num_nodes=3, time_bucket_hours=1, data_dir=str(tmp_path),
        )
        router = PartitionRouter(config)
        manager = PartitionManager(config)
        optimizer = QueryOptimizer(router, manager)

        sources = ["api_gateway", "web_server", "database"]
        hours = [10, 11, 12, 13]

        for source in sources:
            for hour in hours:
                entry = _make_entry(
                    source=source,
                    timestamp=f"2026-02-28T{hour:02d}:30:00",
                    message=f"{source}-hour-{hour}",
                )
                pid = router.route(entry)
                manager.store(pid, entry)

        # 3 sources x 4 hours = 12 hybrid partitions
        return router, manager, optimizer, sources, hours

    def test_hybrid_source_pruning(self, hybrid_setup):
        """Source filter prunes to only partitions with the matching source component."""
        router, manager, optimizer, sources, hours = hybrid_setup
        total = len(manager.get_all_partition_ids())
        assert total == 12  # 3 sources x 4 hours

        result = optimizer.optimize({"source": "web_server"})

        # web_server hashes to partition 1; should keep 4 partitions (one per hour)
        assert result["partitions_scanned"] == 4
        assert result["total_partitions"] == 12
        assert result["pruned"] == 8
        assert result["improvement_factor"] == 3.0

        source_part = _source_partition("web_server")
        for pid in result["partition_ids"]:
            assert pid.startswith(f"{source_part}_")

    def test_hybrid_time_pruning(self, hybrid_setup):
        """Time filter prunes to only partitions with the matching time component."""
        router, manager, optimizer, sources, hours = hybrid_setup

        result = optimizer.optimize({
            "time_range": {
                "start": "2026-02-28T10:00:00",
                "end": "2026-02-28T10:59:00",
            }
        })

        # 1 hour x 3 sources = 3 partitions
        assert result["partitions_scanned"] == 3
        assert result["total_partitions"] == 12
        assert result["pruned"] == 9
        assert result["improvement_factor"] == 4.0

        for pid in result["partition_ids"]:
            assert pid.endswith("_20260228_10")


# --- Calculation & metrics tests ---------------------------------------------

class TestMetrics:
    def test_improvement_factor_calculation(self, source_optimizer, source_router, source_manager):
        """Verify improvement_factor equals total / scanned."""
        _populate_source_manager(source_router, source_manager)

        result = source_optimizer.optimize({"source": "database"})

        expected = result["total_partitions"] / result["partitions_scanned"]
        assert result["improvement_factor"] == round(expected, 2)

    def test_efficiency_metrics_tracking(self, source_optimizer, source_router, source_manager):
        """Running multiple queries tracks correct aggregate metrics."""
        _populate_source_manager(source_router, source_manager)

        # Query 1: source filter -> scans 1/3, improvement 3.0
        source_optimizer.optimize({"source": "web_server"})
        # Query 2: no filter -> scans 3/3, improvement 1.0
        source_optimizer.optimize({})
        # Query 3: another source filter -> scans 1/3, improvement 3.0
        source_optimizer.optimize({"source": "api_gateway"})

        metrics = source_optimizer.get_efficiency_metrics()

        assert metrics["total_queries"] == 3
        # avg improvement = (3.0 + 1.0 + 3.0) / 3 = 2.33
        assert metrics["avg_improvement_factor"] == round((3.0 + 1.0 + 3.0) / 3, 2)
        # scanned pcts: 33.33, 100.0, 33.33 => avg ~55.55
        expected_avg_pct = round((100 / 3 + 100.0 + 100 / 3) / 3, 2)
        assert metrics["avg_partitions_scanned_pct"] == expected_avg_pct
        # total pruned = 2 + 0 + 2 = 4
        assert metrics["total_partitions_pruned"] == 4

    def test_empty_manager_optimization(self, source_optimizer):
        """Empty manager returns zero-count result with improvement 1.0."""
        result = source_optimizer.optimize({"source": "web_server"})

        assert result["partition_ids"] == []
        assert result["total_partitions"] == 0
        assert result["partitions_scanned"] == 0
        assert result["pruned"] == 0
        assert result["improvement_factor"] == 1.0

    def test_empty_metrics(self):
        """Metrics with no queries returns zeroed-out dict."""
        config = PartitionConfig(strategy="source", num_nodes=3)
        router = PartitionRouter(config)
        manager = PartitionManager(config)
        optimizer = QueryOptimizer(router, manager)

        metrics = optimizer.get_efficiency_metrics()

        assert metrics["total_queries"] == 0
        assert metrics["avg_improvement_factor"] == 0.0
        assert metrics["avg_partitions_scanned_pct"] == 0.0
        assert metrics["total_partitions_pruned"] == 0
