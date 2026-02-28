"""Tests for cluster configuration loading and generation."""

import json
import os

import pytest

from src.config import ClusterConfig, generate_cluster_config, load_config


class TestClusterConfigDefaults:
    """Verify default values on the ClusterConfig dataclass."""

    def test_default_config(self):
        config = ClusterConfig(node_id="node1")

        assert config.node_id == "node1"
        assert config.host == "0.0.0.0"
        assert config.port == 5001
        assert config.storage_dir == "/data"
        assert config.cluster_nodes == []
        assert config.replication_factor == 2
        assert config.health_check_interval == 10
        assert config.quorum_size == 2


class TestLoadConfig:
    """Verify load_config reads from environment variables correctly."""

    def test_load_config_from_env(self, monkeypatch):
        monkeypatch.setenv("NODE_ID", "env-node")
        monkeypatch.setenv("HOST", "192.168.1.1")
        monkeypatch.setenv("PORT", "9000")
        monkeypatch.setenv("STORAGE_DIR", "/mnt/storage")
        monkeypatch.setenv("REPLICATION_FACTOR", "3")
        monkeypatch.setenv("HEALTH_CHECK_INTERVAL", "30")
        monkeypatch.setenv("QUORUM_SIZE", "3")
        monkeypatch.setenv(
            "CLUSTER_NODES",
            json.dumps([{"id": "n1", "host": "h1", "port": 8001}]),
        )

        config = load_config()

        assert config.node_id == "env-node"
        assert config.host == "192.168.1.1"
        assert config.port == 9000
        assert config.storage_dir == "/mnt/storage"
        assert config.replication_factor == 3
        assert config.health_check_interval == 30
        assert config.quorum_size == 3
        assert len(config.cluster_nodes) == 1
        assert config.cluster_nodes[0]["id"] == "n1"

    def test_load_config_missing_node_id(self, monkeypatch):
        monkeypatch.delenv("NODE_ID", raising=False)

        with pytest.raises(ValueError, match="NODE_ID"):
            load_config()

    def test_cluster_nodes_parsing(self, monkeypatch):
        nodes = [
            {"id": "node1", "host": "localhost", "port": 5001},
            {"id": "node2", "host": "localhost", "port": 5002},
            {"id": "node3", "host": "localhost", "port": 5003},
        ]
        monkeypatch.setenv("NODE_ID", "node1")
        monkeypatch.setenv("CLUSTER_NODES", json.dumps(nodes))

        config = load_config()

        assert len(config.cluster_nodes) == 3
        assert config.cluster_nodes[0]["id"] == "node1"
        assert config.cluster_nodes[1]["host"] == "localhost"
        assert config.cluster_nodes[2]["port"] == 5003

    def test_cluster_nodes_invalid_json(self, monkeypatch):
        monkeypatch.setenv("NODE_ID", "node1")
        monkeypatch.setenv("CLUSTER_NODES", "not-valid-json")

        config = load_config()

        assert config.cluster_nodes == []


class TestGenerateClusterConfig:
    """Verify generate_cluster_config produces correct configurations."""

    def test_generate_cluster_config(self):
        configs = generate_cluster_config(num_nodes=3, base_port=5001)

        assert len(configs) == 3

        for i, cfg in enumerate(configs):
            assert cfg.node_id == f"node{i + 1}"
            assert cfg.port == 5001 + i
            assert cfg.storage_dir == f"/data/node{i + 1}"
            assert len(cfg.cluster_nodes) == 3

    def test_generate_cluster_config_ports(self):
        configs = generate_cluster_config(num_nodes=3, base_port=6000)

        assert configs[0].port == 6000
        assert configs[1].port == 6001
        assert configs[2].port == 6002

    def test_generate_cluster_config_single_node(self):
        configs = generate_cluster_config(num_nodes=1, base_port=5001)

        assert len(configs) == 1
        assert configs[0].replication_factor == 1
        assert configs[0].quorum_size == 1

    def test_generate_cluster_config_all_nodes_know_each_other(self):
        configs = generate_cluster_config(num_nodes=3, base_port=5001)

        for cfg in configs:
            node_ids = {n["id"] for n in cfg.cluster_nodes}
            assert node_ids == {"node1", "node2", "node3"}
