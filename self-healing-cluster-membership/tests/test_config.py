"""Tests for cluster configuration loading."""

import pytest

from src.config import ClusterConfig, load_config


class TestClusterConfigDefaults:
    """Tests for ClusterConfig default values."""

    def test_default_node_id(self):
        config = ClusterConfig()
        assert config.node_id == "node-1"

    def test_default_address(self):
        config = ClusterConfig()
        assert config.address == "0.0.0.0"

    def test_default_port(self):
        config = ClusterConfig()
        assert config.port == 5000

    def test_default_role(self):
        config = ClusterConfig()
        assert config.role == "worker"

    def test_default_gossip_interval(self):
        config = ClusterConfig()
        assert config.gossip_interval == 2.0

    def test_default_health_check_interval(self):
        config = ClusterConfig()
        assert config.health_check_interval == 1.0

    def test_default_phi_threshold(self):
        config = ClusterConfig()
        assert config.phi_threshold == 8.0

    def test_default_gossip_fanout(self):
        config = ClusterConfig()
        assert config.gossip_fanout == 3

    def test_default_seed_nodes_empty(self):
        config = ClusterConfig()
        assert config.seed_nodes == []

    def test_default_suspected_health_check_multiplier(self):
        config = ClusterConfig()
        assert config.suspected_health_check_multiplier == 0.5

    def test_default_heartbeat_window_size(self):
        config = ClusterConfig()
        assert config.heartbeat_window_size == 20

    def test_default_cleanup_interval(self):
        config = ClusterConfig()
        assert config.cleanup_interval == 30.0


class TestLoadConfig:
    """Tests for load_config reading from environment variables."""

    def test_load_all_env_vars(self, monkeypatch):
        """load_config reads every supported env var."""
        monkeypatch.setenv("NODE_ID", "env-node")
        monkeypatch.setenv("ADDRESS", "192.168.1.1")
        monkeypatch.setenv("PORT", "6000")
        monkeypatch.setenv("ROLE", "leader")
        monkeypatch.setenv("GOSSIP_INTERVAL", "5.0")
        monkeypatch.setenv("HEALTH_CHECK_INTERVAL", "2.5")
        monkeypatch.setenv("PHI_THRESHOLD", "12.0")
        monkeypatch.setenv("GOSSIP_FANOUT", "5")
        monkeypatch.setenv("SEED_NODES", "host-a:5000,host-b:5001,host-c:5002")
        monkeypatch.setenv("SUSPECTED_HEALTH_CHECK_MULTIPLIER", "0.25")
        monkeypatch.setenv("HEARTBEAT_WINDOW_SIZE", "50")
        monkeypatch.setenv("CLEANUP_INTERVAL", "60.0")

        config = load_config()

        assert config.node_id == "env-node"
        assert config.address == "192.168.1.1"
        assert config.port == 6000
        assert config.role == "leader"
        assert config.gossip_interval == 5.0
        assert config.health_check_interval == 2.5
        assert config.phi_threshold == 12.0
        assert config.gossip_fanout == 5
        assert config.seed_nodes == ["host-a:5000", "host-b:5001", "host-c:5002"]
        assert config.suspected_health_check_multiplier == 0.25
        assert config.heartbeat_window_size == 50
        assert config.cleanup_interval == 60.0

    def test_seed_nodes_comma_separated(self, monkeypatch):
        """SEED_NODES is parsed as comma-separated host:port pairs."""
        monkeypatch.setenv("SEED_NODES", "alpha:5000, beta:5001")
        config = load_config()
        assert config.seed_nodes == ["alpha:5000", "beta:5001"]

    def test_seed_nodes_empty(self, monkeypatch):
        """Empty SEED_NODES produces an empty list."""
        monkeypatch.setenv("SEED_NODES", "")
        config = load_config()
        assert config.seed_nodes == []

    def test_seed_nodes_not_set(self, monkeypatch):
        """Missing SEED_NODES env var produces an empty list."""
        monkeypatch.delenv("SEED_NODES", raising=False)
        config = load_config()
        assert config.seed_nodes == []

    def test_defaults_when_no_env(self, monkeypatch):
        """Without any env vars, load_config returns sane defaults."""
        for var in [
            "NODE_ID", "ADDRESS", "PORT", "ROLE", "GOSSIP_INTERVAL",
            "HEALTH_CHECK_INTERVAL", "PHI_THRESHOLD", "GOSSIP_FANOUT",
            "SEED_NODES", "SUSPECTED_HEALTH_CHECK_MULTIPLIER",
            "HEARTBEAT_WINDOW_SIZE", "CLEANUP_INTERVAL",
        ]:
            monkeypatch.delenv(var, raising=False)

        config = load_config()
        assert config.node_id == "node-1"
        assert config.port == 5000
        assert config.role == "worker"
        assert config.seed_nodes == []
