"""Tests for the FileStore storage engine."""

import json

import pytest

from src.file_store import FileStore


class TestFileStoreWrite:
    """Verify write operations and their side effects."""

    def test_write_returns_expected_keys(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")
        result = store.write({"message": "hello"})

        assert "file_path" in result
        assert "checksum" in result
        assert "version" in result
        assert result["version"] == 1

    def test_write_creates_file(self, tmp_path):
        data_dir = tmp_path / "data"
        store = FileStore(str(data_dir), "test-node")
        result = store.write({"message": "hello"})

        full_path = data_dir / result["file_path"]
        assert full_path.exists()

        with open(full_path) as f:
            record = json.load(f)
        assert record["data"]["message"] == "hello"
        assert record["metadata"]["node_id"] == "test-node"


class TestFileStoreRead:
    """Verify read operations."""

    def test_read_written_file(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")
        result = store.write({"key": "value"})

        record = store.read(result["file_path"])

        assert record is not None
        assert record["data"]["key"] == "value"
        assert record["metadata"]["checksum"] == result["checksum"]

    def test_read_nonexistent_returns_none(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")

        assert store.read("nonexistent.json") is None


class TestFileStoreListFiles:
    """Verify file listing."""

    def test_list_files(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")
        store.write({"a": 1})
        store.write({"b": 2})
        store.write({"c": 3})

        files = store.list_files()

        assert len(files) == 3
        assert all(f.endswith(".json") for f in files)


class TestFileStoreReplica:
    """Verify replica write and read-back."""

    def test_write_replica(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")
        metadata = {"version": 1, "checksum": "abc123", "node_id": "other-node"}

        result = store.write_replica("replica_file.json", {"msg": "replicated"}, metadata)

        assert result["status"] == "replicated"

        record = store.read("replica_file.json")
        assert record is not None
        assert record["data"]["msg"] == "replicated"
        assert record["metadata"]["node_id"] == "other-node"


class TestFileStoreStats:
    """Verify operation counters."""

    def test_stats_tracking(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")

        stats = store.get_stats()
        assert stats["writes"] == 0
        assert stats["reads"] == 0
        assert stats["replications_received"] == 0

        result = store.write({"x": 1})
        store.write({"y": 2})
        store.read(result["file_path"])
        store.write_replica("rep.json", {}, {})

        stats = store.get_stats()
        assert stats["writes"] == 2
        assert stats["reads"] == 1
        assert stats["replications_received"] == 1


class TestFileStoreChecksum:
    """Verify checksum determinism."""

    def test_checksum_consistency(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "test-node")

        data = {"level": "info", "message": "consistent"}
        r1 = store.write(data)
        r2 = store.write(data)

        assert r1["checksum"] == r2["checksum"]
