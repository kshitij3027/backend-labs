"""Tests for the ClusterClient."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests as real_requests

from src.cluster_client import ClusterClient, ClusterUnavailableError


@pytest.fixture
def cluster_nodes():
    return [
        {"id": "node1", "host": "localhost", "port": 5001},
        {"id": "node2", "host": "localhost", "port": 5002},
        {"id": "node3", "host": "localhost", "port": 5003},
    ]


@patch("src.cluster_client.requests.get")
@patch("src.cluster_client.requests.post")
def test_write_to_healthy_node(mock_post, mock_get, cluster_nodes):
    """Write succeeds when a healthy node accepts the write."""
    # Health check: only node1 is healthy
    def health_side_effect(url, **kwargs):
        mock_resp = MagicMock()
        if "5001" in url:
            mock_resp.status_code = 200
        else:
            raise real_requests.ConnectionError("refused")
        return mock_resp

    mock_get.side_effect = health_side_effect

    write_resp = MagicMock()
    write_resp.status_code = 201
    write_resp.json.return_value = {
        "file_path": "log_001.json",
        "checksum": "abc123",
        "version": 1,
    }
    mock_post.return_value = write_resp

    client = ClusterClient(cluster_nodes=cluster_nodes)
    result = client.write({"message": "test", "level": "info"})

    assert result["file_path"] == "log_001.json"
    assert result["checksum"] == "abc123"
    assert result["version"] == 1
    assert result["node_id"] == "node1"
    mock_post.assert_called_once()


@patch("src.cluster_client.requests.get")
@patch("src.cluster_client.requests.post")
def test_write_retries_on_failure(mock_post, mock_get, cluster_nodes):
    """Write retries with next healthy node when first write fails."""
    # All nodes healthy
    health_resp = MagicMock()
    health_resp.status_code = 200
    mock_get.return_value = health_resp

    # First write fails, second succeeds
    fail_resp = MagicMock()
    fail_resp.status_code = 500

    success_resp = MagicMock()
    success_resp.status_code = 201
    success_resp.json.return_value = {
        "file_path": "log_002.json",
        "checksum": "def456",
        "version": 1,
    }
    mock_post.side_effect = [fail_resp, success_resp]

    client = ClusterClient(cluster_nodes=cluster_nodes)
    # Patch shuffle to keep order deterministic
    with patch("src.cluster_client.random.shuffle"):
        result = client.write({"message": "retry test"})

    assert result["file_path"] == "log_002.json"
    assert mock_post.call_count == 2


@patch("src.cluster_client.requests.get")
def test_write_raises_when_all_down(mock_get, cluster_nodes):
    """ClusterUnavailableError raised when no nodes respond to health check."""
    mock_get.side_effect = real_requests.ConnectionError("refused")

    client = ClusterClient(cluster_nodes=cluster_nodes)
    with pytest.raises(ClusterUnavailableError, match="No healthy nodes"):
        client.write({"message": "fail"})


@patch("src.cluster_client.requests.get")
@patch("src.cluster_client.requests.post")
def test_write_handles_no_quorum_503(mock_post, mock_get, cluster_nodes):
    """Write retries when a node returns 503 (no quorum)."""
    health_resp = MagicMock()
    health_resp.status_code = 200
    mock_get.return_value = health_resp

    quorum_fail = MagicMock()
    quorum_fail.status_code = 503

    success_resp = MagicMock()
    success_resp.status_code = 201
    success_resp.json.return_value = {
        "file_path": "log_003.json",
        "checksum": "ghi789",
        "version": 1,
    }
    mock_post.side_effect = [quorum_fail, success_resp]

    client = ClusterClient(cluster_nodes=cluster_nodes)
    with patch("src.cluster_client.random.shuffle"):
        result = client.write({"message": "quorum test"})

    assert result["file_path"] == "log_003.json"
    assert mock_post.call_count == 2


@patch("src.cluster_client.requests.get")
def test_read_from_cluster(mock_get, cluster_nodes):
    """Read returns file data from the first node that has it."""

    def side_effect(url, **kwargs):
        mock_resp = MagicMock()
        if "/health" in url:
            mock_resp.status_code = 200
        elif "/read/" in url:
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                "data": {"message": "hello"},
                "metadata": {"version": 1},
            }
        return mock_resp

    mock_get.side_effect = side_effect

    client = ClusterClient(cluster_nodes=cluster_nodes)
    result = client.read("log_001.json")

    assert result["data"]["message"] == "hello"
    assert result["metadata"]["version"] == 1


@patch("src.cluster_client.requests.get")
def test_read_not_found(mock_get, cluster_nodes):
    """FileNotFoundError raised when no node has the file."""

    def side_effect(url, **kwargs):
        mock_resp = MagicMock()
        if "/health" in url:
            mock_resp.status_code = 200
        elif "/read/" in url:
            mock_resp.status_code = 404
        return mock_resp

    mock_get.side_effect = side_effect

    client = ClusterClient(cluster_nodes=cluster_nodes)
    with pytest.raises(FileNotFoundError, match="not found on any node"):
        client.read("nonexistent.json")


@patch("src.cluster_client.requests.get")
def test_health_returns_status(mock_get, cluster_nodes):
    """Health check returns correct structure with mixed node states."""

    def side_effect(url, **kwargs):
        mock_resp = MagicMock()
        if "5001" in url or "5002" in url:
            mock_resp.status_code = 200
        else:
            raise real_requests.ConnectionError("refused")
        return mock_resp

    mock_get.side_effect = side_effect

    client = ClusterClient(cluster_nodes=cluster_nodes)
    result = client.health()

    assert result["healthy_nodes"] == 2
    assert result["total_nodes"] == 3
    assert result["quorum"] is True
    assert len(result["nodes"]) == 3

    statuses = {n["id"]: n["status"] for n in result["nodes"]}
    assert statuses["node1"] == "healthy"
    assert statuses["node2"] == "healthy"
    assert statuses["node3"] == "unhealthy"


def test_cluster_unavailable_on_empty_nodes():
    """ClusterUnavailableError raised when node list is empty."""
    client = ClusterClient(cluster_nodes=[])
    with pytest.raises(ClusterUnavailableError, match="No healthy nodes"):
        client.write({"message": "empty"})


def test_client_reads_from_env():
    """ClusterClient parses CLUSTER_NODES from environment variable."""
    nodes = [{"id": "env-node", "host": "envhost", "port": 9999}]
    env_val = json.dumps(nodes)

    with patch.dict(os.environ, {"CLUSTER_NODES": env_val}):
        client = ClusterClient()

    assert len(client.cluster_nodes) == 1
    assert client.cluster_nodes[0]["id"] == "env-node"
    assert client.cluster_nodes[0]["host"] == "envhost"
    assert client.cluster_nodes[0]["port"] == 9999
