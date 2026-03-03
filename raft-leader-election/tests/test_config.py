"""Tests for configuration loading."""

import os
import pytest
from src.config import RaftConfig, load_config


class TestRaftConfig:
    """Test RaftConfig dataclass."""

    def test_default_config(self):
        config = RaftConfig()
        assert config.node_id == "node-1"
        assert config.host == "0.0.0.0"
        assert config.port == 5001
        assert config.peers == []
        assert config.election_timeout_min == 150
        assert config.election_timeout_max == 300
        assert config.heartbeat_interval == 50
        assert config.priority == 1

    def test_cluster_size_no_peers(self):
        config = RaftConfig(peers=[])
        assert config.cluster_size == 1

    def test_cluster_size_with_peers(self):
        config = RaftConfig(peers=["node-2:5002", "node-3:5003"])
        assert config.cluster_size == 3

    def test_majority_single_node(self):
        config = RaftConfig(peers=[])
        assert config.majority == 1

    def test_majority_three_nodes(self):
        config = RaftConfig(peers=["node-2:5002", "node-3:5003"])
        assert config.majority == 2

    def test_majority_five_nodes(self):
        config = RaftConfig(
            peers=["n2:5002", "n3:5003", "n4:5004", "n5:5005"]
        )
        assert config.majority == 3


class TestLoadConfig:
    """Test load_config from environment variables."""

    def test_load_defaults(self, monkeypatch):
        monkeypatch.delenv("NODE_ID", raising=False)
        monkeypatch.delenv("PEERS", raising=False)
        monkeypatch.delenv("PORT", raising=False)
        config = load_config()
        assert config.node_id == "node-1"
        assert config.peers == []
        assert config.port == 5001

    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("NODE_ID", "node-3")
        monkeypatch.setenv("PORT", "5003")
        monkeypatch.setenv("PEERS", "node-1:5001,node-2:5002")
        monkeypatch.setenv("ELECTION_TIMEOUT_MIN", "200")
        monkeypatch.setenv("ELECTION_TIMEOUT_MAX", "400")
        monkeypatch.setenv("HEARTBEAT_INTERVAL", "75")
        monkeypatch.setenv("PRIORITY", "5")
        config = load_config()
        assert config.node_id == "node-3"
        assert config.port == 5003
        assert config.peers == ["node-1:5001", "node-2:5002"]
        assert config.election_timeout_min == 200
        assert config.election_timeout_max == 400
        assert config.heartbeat_interval == 75
        assert config.priority == 5

    def test_load_empty_peers(self, monkeypatch):
        monkeypatch.setenv("PEERS", "")
        config = load_config()
        assert config.peers == []

    def test_load_peers_with_spaces(self, monkeypatch):
        monkeypatch.setenv("PEERS", " node-1:5001 , node-2:5002 ")
        config = load_config()
        assert config.peers == ["node-1:5001", "node-2:5002"]
