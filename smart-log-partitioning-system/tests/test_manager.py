"""Tests for the PartitionManager class."""

import json
import os

import pytest

from src.config import PartitionConfig
from src.manager import PartitionManager


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


class TestStore:
    def test_store_adds_entry_to_partition(self, manager):
        entry = _make_entry()
        manager.store("0", entry)

        assert len(manager.partitions["0"]) == 1
        assert manager.partitions["0"][0] == entry

    def test_store_multiple_partitions(self, manager):
        entry_a = _make_entry(source="web-server", message="entry A")
        entry_b = _make_entry(source="auth-service", message="entry B")
        entry_c = _make_entry(source="db-service", message="entry C")

        manager.store("0", entry_a)
        manager.store("1", entry_b)
        manager.store("0", entry_c)

        assert len(manager.partitions["0"]) == 2
        assert len(manager.partitions["1"]) == 1
        assert manager.partitions["0"][0] == entry_a
        assert manager.partitions["0"][1] == entry_c
        assert manager.partitions["1"][0] == entry_b


class TestQuery:
    def test_query_returns_matching_entries(self, manager):
        entry_a = _make_entry(message="alpha")
        entry_b = _make_entry(message="beta")
        entry_c = _make_entry(message="gamma")

        manager.store("0", entry_a)
        manager.store("1", entry_b)
        manager.store("0", entry_c)

        results = manager.query(["0"])
        assert len(results) == 2
        assert results[0]["message"] == "alpha"
        assert results[1]["message"] == "gamma"

        results_both = manager.query(["0", "1"])
        assert len(results_both) == 3

    def test_query_filter_by_source(self, manager):
        manager.store("0", _make_entry(source="web-server", message="web log"))
        manager.store("0", _make_entry(source="auth-service", message="auth log"))
        manager.store("0", _make_entry(source="web-server", message="web log 2"))

        results = manager.query(["0"], filters={"source": "web-server"})
        assert len(results) == 2
        assert all(r["source"] == "web-server" for r in results)

    def test_query_filter_by_level(self, manager):
        manager.store("0", _make_entry(level="INFO", message="info msg"))
        manager.store("0", _make_entry(level="ERROR", message="error msg"))
        manager.store("0", _make_entry(level="INFO", message="info msg 2"))
        manager.store("0", _make_entry(level="WARN", message="warn msg"))

        results = manager.query(["0"], filters={"level": "ERROR"})
        assert len(results) == 1
        assert results[0]["level"] == "ERROR"

    def test_query_filter_by_time_range(self, manager):
        manager.store("0", _make_entry(timestamp="2026-02-28T08:00:00", message="early"))
        manager.store("0", _make_entry(timestamp="2026-02-28T10:00:00", message="mid"))
        manager.store("0", _make_entry(timestamp="2026-02-28T12:00:00", message="noon"))
        manager.store("0", _make_entry(timestamp="2026-02-28T18:00:00", message="late"))

        results = manager.query(
            ["0"],
            filters={
                "time_range": {
                    "start": "2026-02-28T09:00:00",
                    "end": "2026-02-28T13:00:00",
                }
            },
        )
        assert len(results) == 2
        messages = [r["message"] for r in results]
        assert "mid" in messages
        assert "noon" in messages


class TestPersistence:
    def test_jsonl_persistence(self, manager, tmp_path):
        entry_a = _make_entry(message="first")
        entry_b = _make_entry(message="second")

        manager.store("0", entry_a)
        manager.store("0", entry_b)

        filepath = os.path.join(str(tmp_path), "partition_0.jsonl")
        assert os.path.exists(filepath)

        with open(filepath, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) == 2
        assert json.loads(lines[0])["message"] == "first"
        assert json.loads(lines[1])["message"] == "second"

    def test_load_from_disk(self, tmp_path):
        config = PartitionConfig(data_dir=str(tmp_path))

        # First manager stores data
        manager1 = PartitionManager(config)
        manager1.store("0", _make_entry(message="persisted-a"))
        manager1.store("1", _make_entry(message="persisted-b"))
        manager1.store("0", _make_entry(message="persisted-c"))

        # Second manager loads from disk
        manager2 = PartitionManager(config)
        assert len(manager2.partitions) == 0  # starts empty in memory

        manager2.load_from_disk()
        assert len(manager2.partitions["0"]) == 2
        assert len(manager2.partitions["1"]) == 1
        assert manager2.partitions["0"][0]["message"] == "persisted-a"
        assert manager2.partitions["0"][1]["message"] == "persisted-c"
        assert manager2.partitions["1"][0]["message"] == "persisted-b"


class TestStats:
    def test_get_stats(self, manager):
        # Create an imbalanced distribution: partition "0" gets 10, "1" gets 2, "2" gets 3
        for i in range(10):
            manager.store("0", _make_entry(message=f"msg-{i}"))
        for i in range(2):
            manager.store("1", _make_entry(message=f"msg-{i}"))
        for i in range(3):
            manager.store("2", _make_entry(message=f"msg-{i}"))

        stats = manager.get_stats()

        assert stats["total_entries"] == 15
        assert stats["num_partitions"] == 3
        assert stats["partitions"]["0"] == 10
        assert stats["partitions"]["1"] == 2
        assert stats["partitions"]["2"] == 3
        assert stats["variance_pct"] > 0
        # Partition "0" has 10 entries, avg is 5, threshold is 7.5 -> "0" is a hotspot
        assert "0" in stats["hotspots"]
        # Partitions "1" and "2" are below threshold
        assert "1" not in stats["hotspots"]
        assert "2" not in stats["hotspots"]

    def test_get_stats_empty(self, manager):
        stats = manager.get_stats()

        assert stats["total_entries"] == 0
        assert stats["num_partitions"] == 0
        assert stats["partitions"] == {}
        assert stats["variance_pct"] == 0.0
        assert stats["hotspots"] == []


class TestUtilities:
    def test_get_all_partition_ids(self, manager):
        manager.store("alpha", _make_entry(message="a"))
        manager.store("beta", _make_entry(message="b"))
        manager.store("gamma", _make_entry(message="c"))

        ids = manager.get_all_partition_ids()
        assert sorted(ids) == ["alpha", "beta", "gamma"]

    def test_clear_removes_all_data(self, manager, tmp_path):
        manager.store("0", _make_entry(message="first"))
        manager.store("1", _make_entry(message="second"))

        # Verify data exists
        assert len(manager.partitions) == 2
        jsonl_files = [f for f in os.listdir(str(tmp_path)) if f.endswith(".jsonl")]
        assert len(jsonl_files) == 2

        manager.clear()

        # In-memory data gone
        assert len(manager.partitions) == 0

        # JSONL files removed
        jsonl_files = [f for f in os.listdir(str(tmp_path)) if f.endswith(".jsonl")]
        assert len(jsonl_files) == 0
