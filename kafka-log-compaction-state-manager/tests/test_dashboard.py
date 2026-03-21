"""Tests for the Flask dashboard routes."""

import pytest
from unittest.mock import MagicMock, patch

from src.config import Settings
from src.dashboard import create_app
from src.models import UserProfile


@pytest.fixture
def mock_consumer():
    consumer = MagicMock()
    consumer.get_stats.return_value = {
        "total_consumed": 42,
        "active_profiles": 5,
        "total_in_state": 8,
        "tombstones_processed": 3,
        "updates_by_type": {"CREATE": 10, "UPDATE": 28, "DELETE": 4},
    }
    consumer.get_active_profiles.return_value = {
        "user_abc": UserProfile(
            user_id="user_abc",
            email="alice@example.com",
            first_name="Alice",
            last_name="Smith",
            age=30,
            version=2,
            last_updated="2025-01-01T00:00:00+00:00",
        )
    }
    return consumer


@pytest.fixture
def mock_monitor():
    monitor = MagicMock()
    monitor.get_compaction_metrics.return_value = {
        "total_messages": 100,
        "unique_keys": 8,
        "compaction_ratio": 0.08,
        "estimated_storage_saved_bytes": 6800,
        "messages_per_second": 1.5,
    }
    monitor.get_topic_config.return_value = {
        "cleanup.policy": "compact",
        "segment.bytes": "1048576",
    }
    return monitor


@pytest.fixture
def client(mock_consumer, mock_monitor):
    config = Settings()
    app = create_app(config, mock_consumer, mock_monitor)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


class TestHealthEndpoint:
    def test_returns_200_with_status_healthy(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert data["service"] == "kafka-log-compaction-state-manager"


class TestStatsEndpoint:
    def test_returns_200_with_expected_keys(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "active_profiles" in data
        assert "total_consumed" in data
        assert "tombstones_processed" in data
        assert "compaction_metrics" in data
        assert "topic_config" in data

    def test_compaction_metrics_structure(self, client):
        resp = client.get("/api/stats")
        cm = resp.get_json()["compaction_metrics"]
        assert "total_messages" in cm
        assert "unique_keys" in cm
        assert "compaction_ratio" in cm
        assert "estimated_storage_saved_bytes" in cm
        assert "messages_per_second" in cm


class TestProfilesEndpoint:
    def test_returns_200_with_list(self, client):
        resp = client.get("/api/profiles")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["user_id"] == "user_abc"

    def test_profile_by_id_not_found(self, client):
        resp = client.get("/api/profiles/nonexistent")
        assert resp.status_code == 404

    def test_profile_by_id_found(self, client):
        resp = client.get("/api/profiles/user_abc")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["user_id"] == "user_abc"
        assert data["email"] == "alice@example.com"


class TestIndexPage:
    def test_returns_200_with_title(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"Kafka Log Compaction" in resp.data
