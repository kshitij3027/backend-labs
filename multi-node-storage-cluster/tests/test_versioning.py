"""Tests for the VersionManager — version tracking and read-repair."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.config import ClusterConfig
from src.file_store import FileStore
from src.versioning import VersionManager


@pytest.fixture
def version_setup(tmp_path):
    """Provide a ClusterConfig and VersionManager for testing."""
    config = ClusterConfig(
        node_id="node1",
        port=5001,
        storage_dir=str(tmp_path),
        cluster_nodes=[
            {"id": "node1", "host": "localhost", "port": 5001},
            {"id": "node2", "host": "localhost", "port": 5002},
        ],
    )
    vm = VersionManager(config)
    return config, vm


class TestNextVersion:
    """Verify atomic version increment."""

    def test_next_version_increments(self, version_setup):
        _, vm = version_setup

        assert vm.next_version("file_a.json") == 1
        assert vm.next_version("file_a.json") == 2
        assert vm.next_version("file_a.json") == 3

    def test_next_version_independent_per_file(self, version_setup):
        _, vm = version_setup

        assert vm.next_version("file_a.json") == 1
        assert vm.next_version("file_b.json") == 1
        assert vm.next_version("file_a.json") == 2
        assert vm.next_version("file_b.json") == 2


class TestGetVersion:
    """Verify version retrieval."""

    def test_get_version_default_zero(self, version_setup):
        _, vm = version_setup

        assert vm.get_version("unknown_file.json") == 0

    def test_get_version_after_increment(self, version_setup):
        _, vm = version_setup

        vm.next_version("file.json")
        vm.next_version("file.json")

        assert vm.get_version("file.json") == 2


class TestSetVersion:
    """Verify version setter (used for replica tracking)."""

    def test_set_version(self, version_setup):
        _, vm = version_setup

        vm.set_version("file.json", 5)

        assert vm.get_version("file.json") == 5

    def test_set_version_only_increases(self, version_setup):
        _, vm = version_setup

        vm.set_version("file.json", 5)
        vm.set_version("file.json", 3)

        assert vm.get_version("file.json") == 5

    def test_set_version_allows_higher(self, version_setup):
        _, vm = version_setup

        vm.set_version("file.json", 5)
        vm.set_version("file.json", 8)

        assert vm.get_version("file.json") == 8


class TestReadRepair:
    """Verify background read-repair behaviour."""

    @patch("src.versioning.requests")
    def test_read_repair_pushes_to_stale_peer(self, mock_requests, version_setup):
        config, vm = version_setup

        # Mock GET — peer returns older version
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.json.return_value = {
            "data": {"msg": "old"},
            "metadata": {"version": 1},
        }
        mock_requests.get.return_value = mock_get_resp

        # Mock POST — push succeeds
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 201
        mock_requests.post.return_value = mock_post_resp

        local_record = {
            "data": {"msg": "new"},
            "metadata": {"version": 3},
        }

        # Run synchronously for testing
        vm._do_read_repair("file.json", local_record, config.cluster_nodes, "node1")

        # Should have called GET on the peer
        mock_requests.get.assert_called_once()
        call_url = mock_requests.get.call_args[0][0]
        assert "node2" not in call_url or "5002" in call_url

        # Should have pushed to stale peer via POST
        mock_requests.post.assert_called_once()
        post_url = mock_requests.post.call_args[0][0]
        assert "5002" in post_url
        assert "/replicate" in post_url

    @patch("src.versioning.requests")
    def test_read_repair_skips_self(self, mock_requests, version_setup):
        config, vm = version_setup

        local_record = {
            "data": {"msg": "data"},
            "metadata": {"version": 1},
        }

        # Peer returns same version — no push needed
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.json.return_value = {
            "data": {"msg": "data"},
            "metadata": {"version": 1},
        }
        mock_requests.get.return_value = mock_get_resp

        vm._do_read_repair("file.json", local_record, config.cluster_nodes, "node1")

        # Only one GET call (to node2), not two (self should be skipped)
        assert mock_requests.get.call_count == 1
        call_url = mock_requests.get.call_args[0][0]
        assert "5002" in call_url

    @patch("src.versioning.requests")
    def test_read_repair_pushes_missing_file(self, mock_requests, version_setup):
        config, vm = version_setup

        # Mock GET — peer returns 404
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 404
        mock_requests.get.return_value = mock_get_resp

        # Mock POST — push succeeds
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 201
        mock_requests.post.return_value = mock_post_resp

        local_record = {
            "data": {"msg": "exists here"},
            "metadata": {"version": 2},
        }

        vm._do_read_repair("file.json", local_record, config.cluster_nodes, "node1")

        # Should push to the peer that doesn't have the file
        mock_requests.post.assert_called_once()
        payload = mock_requests.post.call_args[1]["json"]
        assert payload["file_path"] == "file.json"
        assert payload["data"]["msg"] == "exists here"

    @patch("src.versioning.requests")
    def test_read_repair_no_push_when_peer_is_current(self, mock_requests, version_setup):
        config, vm = version_setup

        # Peer has the same version — no push needed
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.json.return_value = {
            "data": {"msg": "same"},
            "metadata": {"version": 2},
        }
        mock_requests.get.return_value = mock_get_resp

        local_record = {
            "data": {"msg": "same"},
            "metadata": {"version": 2},
        }

        vm._do_read_repair("file.json", local_record, config.cluster_nodes, "node1")

        # No POST should have been made
        mock_requests.post.assert_not_called()


class TestVersionManagerWithFileStore:
    """Integration: verify FileStore uses VersionManager for versioning."""

    def test_version_manager_with_file_store(self, tmp_path):
        config = ClusterConfig(
            node_id="node1",
            port=5001,
            storage_dir=str(tmp_path),
        )
        vm = VersionManager(config)
        store = FileStore(str(tmp_path / "data"), "node1", version_manager=vm)

        r1 = store.write({"msg": "first"})
        r2 = store.write({"msg": "second"})

        # Each write to a different file should get version 1
        assert r1["version"] == 1
        assert r2["version"] == 1

        # Verify the version is stored in the file metadata
        record1 = store.read(r1["file_path"])
        assert record1["metadata"]["version"] == 1

        record2 = store.read(r2["file_path"])
        assert record2["metadata"]["version"] == 1

    def test_file_store_without_version_manager(self, tmp_path):
        store = FileStore(str(tmp_path / "data"), "node1")

        r1 = store.write({"msg": "first"})

        assert r1["version"] == 1

    def test_write_replica_tracks_version(self, tmp_path):
        config = ClusterConfig(
            node_id="node1",
            port=5001,
            storage_dir=str(tmp_path),
        )
        vm = VersionManager(config)
        store = FileStore(str(tmp_path / "data"), "node1", version_manager=vm)

        store.write_replica(
            "replica.json",
            {"msg": "replicated"},
            {"version": 5, "node_id": "node2"},
        )

        assert vm.get_version("replica.json") == 5


class TestThreadSafety:
    """Verify concurrent version increments are safe."""

    def test_concurrent_increments(self, version_setup):
        _, vm = version_setup
        results = []

        def increment():
            for _ in range(100):
                v = vm.next_version("shared_file.json")
                results.append(v)

        threads = [threading.Thread(target=increment) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have 500 unique, sequential versions
        assert len(results) == 500
        assert len(set(results)) == 500
        assert vm.get_version("shared_file.json") == 500
