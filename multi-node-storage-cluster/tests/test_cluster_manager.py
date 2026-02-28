"""Tests for ClusterManager health monitoring and quorum enforcement."""

from unittest.mock import MagicMock, patch

import pytest
import requests as req_lib

from src.cluster_manager import ClusterManager
from src.config import ClusterConfig


@pytest.fixture
def cluster_setup(tmp_path):
    """Provide a ClusterManager with a 3-node config and long check interval."""
    config = ClusterConfig(
        node_id="node1",
        port=5001,
        storage_dir=str(tmp_path),
        cluster_nodes=[
            {"id": "node1", "host": "localhost", "port": 5001},
            {"id": "node2", "host": "localhost", "port": 5002},
            {"id": "node3", "host": "localhost", "port": 5003},
        ],
        quorum_size=2,
        health_check_interval=300,  # long interval so loop doesn't interfere
    )
    manager = ClusterManager(config)
    yield config, manager
    manager.shutdown()


class TestInitialState:
    """Verify cluster manager starts with all nodes healthy."""

    def test_initial_state_all_healthy(self, cluster_setup):
        _, manager = cluster_setup

        status = manager.get_cluster_status()
        for node_info in status["nodes"].values():
            assert node_info["status"] == "healthy"

    def test_has_quorum_initially(self, cluster_setup):
        _, manager = cluster_setup

        assert manager.has_quorum() is True


class TestHealthChecks:
    """Verify health check failure detection and recovery."""

    def test_health_check_marks_unhealthy(self, cluster_setup):
        _, manager = cluster_setup

        # Mock requests.get to always fail for peer nodes
        with patch("src.cluster_manager.requests.get", side_effect=req_lib.ConnectionError("connection refused")):
            manager._check_all_nodes()
            manager._check_all_nodes()
            manager._check_all_nodes()

        status = manager.get_cluster_status()
        assert status["nodes"]["node2"]["status"] == "unhealthy"
        assert status["nodes"]["node3"]["status"] == "unhealthy"
        assert status["nodes"]["node2"]["consecutive_failures"] == 3
        assert status["nodes"]["node3"]["consecutive_failures"] == 3

    def test_health_check_recovers(self, cluster_setup):
        _, manager = cluster_setup

        # First, make node2 unhealthy
        with patch("src.cluster_manager.requests.get", side_effect=req_lib.ConnectionError("connection refused")):
            for _ in range(3):
                manager._check_all_nodes()

        assert manager.get_cluster_status()["nodes"]["node2"]["status"] == "unhealthy"

        # Now mock a successful health check
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("src.cluster_manager.requests.get", return_value=mock_resp):
            manager._check_all_nodes()

        assert manager.get_cluster_status()["nodes"]["node2"]["status"] == "healthy"
        assert manager.get_cluster_status()["nodes"]["node2"]["consecutive_failures"] == 0

    def test_self_always_healthy(self, cluster_setup):
        _, manager = cluster_setup

        # Even if requests fail, self node stays healthy
        with patch("src.cluster_manager.requests.get", side_effect=req_lib.ConnectionError("connection refused")):
            for _ in range(5):
                manager._check_all_nodes()

        status = manager.get_cluster_status()
        assert status["nodes"]["node1"]["status"] == "healthy"
        assert status["nodes"]["node1"]["consecutive_failures"] == 0


class TestQuorum:
    """Verify quorum logic under various node failure scenarios."""

    def test_quorum_lost_when_two_nodes_down(self, cluster_setup):
        _, manager = cluster_setup

        # Fail both peer nodes (node2, node3)
        with patch("src.cluster_manager.requests.get", side_effect=req_lib.ConnectionError("connection refused")):
            for _ in range(3):
                manager._check_all_nodes()

        # Only node1 (self) is healthy; quorum_size=2 so quorum is lost
        assert manager.has_quorum() is False

    def test_quorum_with_one_node_down(self, cluster_setup):
        _, manager = cluster_setup

        # Fail only node3 by selectively failing requests
        def selective_fail(url, **kwargs):
            if "5003" in url:
                raise req_lib.ConnectionError("connection refused")
            resp = MagicMock()
            resp.status_code = 200
            return resp

        with patch("src.cluster_manager.requests.get", side_effect=selective_fail):
            for _ in range(3):
                manager._check_all_nodes()

        # node1 (self) + node2 healthy = 2 >= quorum_size=2
        assert manager.has_quorum() is True


class TestNodeQueries:
    """Verify node listing and status reporting."""

    def test_get_healthy_nodes(self, cluster_setup):
        _, manager = cluster_setup

        healthy = manager.get_healthy_nodes()
        assert set(healthy) == {"node1", "node2", "node3"}

    def test_get_healthy_nodes_after_failure(self, cluster_setup):
        _, manager = cluster_setup

        # Fail node3
        def selective_fail(url, **kwargs):
            if "5003" in url:
                raise req_lib.ConnectionError("connection refused")
            resp = MagicMock()
            resp.status_code = 200
            return resp

        with patch("src.cluster_manager.requests.get", side_effect=selective_fail):
            for _ in range(3):
                manager._check_all_nodes()

        healthy = manager.get_healthy_nodes()
        assert "node1" in healthy
        assert "node2" in healthy
        assert "node3" not in healthy

    def test_get_cluster_status_structure(self, cluster_setup):
        _, manager = cluster_setup

        status = manager.get_cluster_status()

        assert "quorum" in status
        assert "healthy_nodes" in status
        assert "total_nodes" in status
        assert "nodes" in status
        assert status["total_nodes"] == 3
        assert status["healthy_nodes"] == 3
        assert status["quorum"] is True

        for nid in ("node1", "node2", "node3"):
            node = status["nodes"][nid]
            assert "status" in node
            assert "last_seen" in node
            assert "consecutive_failures" in node
            assert "host" in node
            assert "port" in node
