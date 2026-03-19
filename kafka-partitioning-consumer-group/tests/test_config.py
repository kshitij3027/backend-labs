"""Tests for configuration loading."""
import os
import pytest
from src.config import Settings, load_config


class TestSettings:
    def test_defaults(self):
        s = Settings()
        assert s.bootstrap_servers == "kafka:29092"
        assert s.topic == "log-processing-topic"
        assert s.num_partitions == 6
        assert s.group_id == "log-processing-group"
        assert s.num_consumers == 3
        assert s.producer_rate == 20
        assert s.duration == 60
        assert s.session_timeout_ms == 10000
        assert s.heartbeat_interval_ms == 3000
        assert s.partition_strategy == "key-based"
        assert s.dashboard_port == 8080
        assert len(s.services) == 6
        assert s.user_id_min == 1000
        assert s.user_id_max == 9999

    def test_log_level_weights_sum_to_one(self):
        s = Settings()
        total = sum(s.log_level_weights.values())
        assert abs(total - 1.0) < 0.001

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("NUM_CONSUMERS", "5")
        monkeypatch.setenv("PRODUCER_RATE", "100")
        s = load_config()
        assert s.bootstrap_servers == "localhost:9092"
        assert s.num_consumers == 5
        assert s.producer_rate == 100

    def test_auto_scale_env(self, monkeypatch):
        monkeypatch.setenv("AUTO_SCALE_ENABLED", "true")
        monkeypatch.setenv("LAG_THRESHOLD", "500")
        s = load_config()
        assert s.auto_scale_enabled is True
        assert s.lag_threshold == 500
