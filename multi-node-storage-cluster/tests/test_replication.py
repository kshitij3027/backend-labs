"""Tests for the ReplicationManager with async replication and hinted handoff."""

from unittest.mock import MagicMock, patch

import pytest

from src.config import ClusterConfig
from src.consistent_hash import HashRing
from src.replication import ReplicationManager


@pytest.fixture
def replication_setup(tmp_path):
    """Provide a ClusterConfig, HashRing, and ReplicationManager for tests."""
    config = ClusterConfig(
        node_id="node1",
        port=5001,
        storage_dir=str(tmp_path),
        cluster_nodes=[
            {"id": "node1", "host": "localhost", "port": 5001},
            {"id": "node2", "host": "localhost", "port": 5002},
            {"id": "node3", "host": "localhost", "port": 5003},
        ],
        replication_factor=2,
    )
    ring = HashRing(["node1", "node2", "node3"])
    manager = ReplicationManager(config, ring)
    yield config, ring, manager
    manager.shutdown()


class TestReplicateSendsToPeers:
    """Verify that _do_replicate sends to the correct peer nodes."""

    @patch("src.replication.requests.post")
    def test_replicate_sends_to_peers(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_post.return_value = mock_resp

        manager._do_replicate(
            "test_file.json",
            {"msg": "hello"},
            {"version": 1, "checksum": "abc123"},
        )

        # Should have called post at least once (for a peer node)
        assert mock_post.called
        # Every call should target a non-self node
        for call in mock_post.call_args_list:
            url = call[1].get("json", call[0][0] if call[0] else "")
            # The URL should not target node1 (self)
            called_url = call[0][0] if call[0] else call[1].get("url", "")
            assert "5001" not in called_url


class TestReplicateSkipsSelf:
    """Verify that _do_replicate never sends a replica to itself."""

    @patch("src.replication.requests.post")
    def test_replicate_skips_self(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_post.return_value = mock_resp

        manager._do_replicate(
            "test_file.json",
            {"msg": "hello"},
            {"version": 1},
        )

        # Verify no call was made to node1's port (5001)
        for call in mock_post.call_args_list:
            called_url = call[0][0] if call[0] else ""
            assert "localhost:5001" not in called_url


class TestFailedReplicationQueuesHint:
    """Verify that a network failure causes a hint to be queued."""

    @patch("src.replication.requests.post")
    def test_failed_replication_queues_hint(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        import requests as req

        mock_post.side_effect = req.ConnectionError("Connection refused")

        manager._do_replicate(
            "test_file.json",
            {"msg": "hello"},
            {"version": 1},
        )

        stats = manager.get_stats()
        assert stats["replications_failed"] > 0
        assert stats["hints_queued"] > 0
        assert stats["hints_pending"] > 0


class TestHintReplay:
    """Verify that pending hints are replayed and stats updated on success."""

    @patch("src.replication.requests.post")
    def test_hint_replay_succeeds(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        # Manually enqueue a hint
        manager._enqueue_hint(
            {"id": "node2", "host": "localhost", "port": 5002},
            "test_file.json",
            {"msg": "hello"},
            {"version": 1},
        )

        assert manager.get_stats()["hints_pending"] == 1

        # Now mock a successful response and replay
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_post.return_value = mock_resp

        manager._replay_hints()

        stats = manager.get_stats()
        assert stats["hints_replayed"] == 1
        assert stats["hints_pending"] == 0

    @patch("src.replication.requests.post")
    def test_hint_replay_requeues_on_failure(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        # Manually enqueue a hint
        manager._enqueue_hint(
            {"id": "node2", "host": "localhost", "port": 5002},
            "test_file.json",
            {"msg": "hello"},
            {"version": 1},
        )

        # Mock a continued failure
        import requests as req

        mock_post.side_effect = req.ConnectionError("Still down")

        manager._replay_hints()

        stats = manager.get_stats()
        assert stats["hints_replayed"] == 0
        # Hint should be re-queued
        assert stats["hints_pending"] == 1


class TestReplicationStats:
    """Verify that statistics counters are tracked correctly."""

    def test_initial_stats(self, replication_setup):
        _config, _ring, manager = replication_setup

        stats = manager.get_stats()
        assert stats["replications_sent"] == 0
        assert stats["replications_failed"] == 0
        assert stats["hints_queued"] == 0
        assert stats["hints_replayed"] == 0
        assert stats["hints_pending"] == 0

    @patch("src.replication.requests.post")
    def test_stats_after_successful_replication(self, mock_post, replication_setup):
        _config, _ring, manager = replication_setup

        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_post.return_value = mock_resp

        manager._do_replicate(
            "test_file.json",
            {"msg": "hello"},
            {"version": 1},
        )

        stats = manager.get_stats()
        assert stats["replications_sent"] > 0
        assert stats["replications_failed"] == 0


class TestShutdown:
    """Verify that shutdown stops the background replay thread."""

    def test_shutdown_sets_running_false(self, replication_setup):
        _config, _ring, manager = replication_setup

        assert manager._running is True
        manager.shutdown()
        assert manager._running is False
