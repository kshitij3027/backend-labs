"""Tests for the configuration module."""

import os

import pytest

from src.config import ClusterConfig, NodeConfig, load_config, load_config_from_env


class TestLoadConfigFromYaml:
    """Tests for loading configuration from YAML files."""

    def test_load_config_from_yaml(self):
        """Load config/cluster.yaml and verify top-level settings."""
        config = load_config("config/cluster.yaml")

        assert config.name == "log-distribution-cluster"
        assert config.virtual_nodes == 150
        assert config.replica_count == 1
        assert len(config.nodes) == 3
        assert config.dashboard_host == "0.0.0.0"
        assert config.dashboard_port == 5000

    def test_load_config_node_details(self):
        """Verify each node has the correct id, host, port, and data_dir."""
        config = load_config("config/cluster.yaml")

        expected = [
            ("node1", "localhost", 5001, "data/node1"),
            ("node2", "localhost", 5002, "data/node2"),
            ("node3", "localhost", 5003, "data/node3"),
        ]

        for node, (exp_id, exp_host, exp_port, exp_dir) in zip(
            config.nodes, expected
        ):
            assert node.id == exp_id
            assert node.host == exp_host
            assert node.port == exp_port
            assert node.data_dir == exp_dir

    def test_load_config_missing_file(self):
        """Loading a non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config("config/nonexistent.yaml")


class TestLoadConfigFromEnv:
    """Tests for loading configuration from environment variables."""

    def test_load_config_from_env_defaults(self):
        """Calling load_config_from_env with no env vars gives sensible defaults."""
        config = load_config_from_env()

        assert config.name == "default-cluster"
        assert config.virtual_nodes == 150
        assert config.replica_count == 1
        assert len(config.nodes) == 3
        assert config.nodes[0].id == "node1"
        assert config.nodes[1].id == "node2"
        assert config.nodes[2].id == "node3"
        assert config.dashboard_host == "0.0.0.0"
        assert config.dashboard_port == 5000

    def test_load_config_from_env_custom(self, monkeypatch):
        """Setting env vars produces a config with custom values."""
        monkeypatch.setenv("CLUSTER_NAME", "my-cluster")
        monkeypatch.setenv("CLUSTER_VIRTUAL_NODES", "200")
        monkeypatch.setenv("CLUSTER_REPLICA_COUNT", "3")
        monkeypatch.setenv("CLUSTER_NODES", "alpha,beta")
        monkeypatch.setenv("DASHBOARD_HOST", "127.0.0.1")
        monkeypatch.setenv("DASHBOARD_PORT", "9090")

        config = load_config_from_env()

        assert config.name == "my-cluster"
        assert config.virtual_nodes == 200
        assert config.replica_count == 3
        assert len(config.nodes) == 2
        assert config.nodes[0].id == "alpha"
        assert config.nodes[1].id == "beta"
        assert config.dashboard_host == "127.0.0.1"
        assert config.dashboard_port == 9090


class TestDataclassDefaults:
    """Tests for dataclass default values."""

    def test_cluster_config_defaults(self):
        """ClusterConfig with no args has sensible defaults."""
        config = ClusterConfig()

        assert config.name == "default-cluster"
        assert config.virtual_nodes == 150
        assert config.replica_count == 1
        assert config.nodes == []
        assert config.dashboard_host == "0.0.0.0"
        assert config.dashboard_port == 5000

    def test_node_config_defaults(self):
        """NodeConfig with only id has sensible defaults for other fields."""
        node = NodeConfig(id="x")

        assert node.id == "x"
        assert node.host == "localhost"
        assert node.port == 5000
        assert node.data_dir == "data"
