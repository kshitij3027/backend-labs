"""Tests for configuration loading."""
import os

import pytest

from src.config import Settings, load_config


class TestSettings:
    def test_defaults(self):
        s = Settings()
        assert s.bootstrap_servers == "kafka:29092"
        assert s.group_id == "log-processing-group"
        assert s.topics == ["web-logs", "app-logs", "error-logs"]
        assert s.enable_auto_commit is False
        assert s.auto_offset_reset == "earliest"
        assert s.batch_size == 100
        assert s.batch_timeout_s == 5.0
        assert s.dashboard_port == 8080
        assert s.sliding_window_seconds == 60
        assert s.session_timeout_ms == 45000
        assert s.heartbeat_interval_ms == 15000

    def test_custom_values(self):
        s = Settings(
            bootstrap_servers="broker:9092",
            group_id="custom-group",
            batch_size=50,
        )
        assert s.bootstrap_servers == "broker:9092"
        assert s.group_id == "custom-group"
        assert s.batch_size == 50


class TestLoadConfig:
    def test_defaults(self):
        s = load_config()
        assert s.bootstrap_servers == "kafka:29092"

    def test_env_overrides(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "my-broker:9092")
        monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "my-group")
        monkeypatch.setenv("BATCH_SIZE", "200")
        monkeypatch.setenv("KAFKA_TOPICS", "topic-a, topic-b")
        monkeypatch.setenv("DASHBOARD_PORT", "9090")

        s = load_config()
        assert s.bootstrap_servers == "my-broker:9092"
        assert s.group_id == "my-group"
        assert s.batch_size == 200
        assert s.topics == ["topic-a", "topic-b"]
        assert s.dashboard_port == 9090

    def test_partial_overrides(self, monkeypatch):
        monkeypatch.setenv("REDIS_PORT", "6380")
        s = load_config()
        assert s.redis_port == 6380
        # Non-overridden values stay default
        assert s.redis_host == "redis"
        assert s.bootstrap_servers == "kafka:29092"
