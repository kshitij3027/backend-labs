import os
from src.config import load_coordinator_config, load_partition_config, CoordinatorConfig, PartitionConfig


class TestCoordinatorConfig:
    def test_defaults(self):
        config = CoordinatorConfig()
        assert config.port == 8080
        assert len(config.partition_urls) == 2
        assert config.max_cache_size == 1000

    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("PORT", "9090")
        monkeypatch.setenv("PARTITION_URLS", "http://p1:8081,http://p2:8082,http://p3:8083")
        monkeypatch.setenv("QUERY_TIMEOUT", "10.0")
        config = load_coordinator_config()
        assert config.port == 9090
        assert len(config.partition_urls) == 3
        assert config.query_timeout == 10.0


class TestPartitionConfig:
    def test_defaults(self):
        config = PartitionConfig()
        assert config.port == 8081
        assert config.partition_id == "partition_1"

    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("PORT", "8082")
        monkeypatch.setenv("PARTITION_ID", "partition_2")
        monkeypatch.setenv("LOG_COUNT", "10000")
        config = load_partition_config()
        assert config.port == 8082
        assert config.partition_id == "partition_2"
        assert config.log_count == 10000
