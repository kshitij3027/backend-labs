"""Tests for web dashboard."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from src.config import Settings


class TestWebDashboard:
    @pytest.fixture
    def client(self):
        """Create a test client with mocked Kafka components."""
        settings = Settings(bootstrap_servers="localhost:9092", duration=0)

        with patch("src.monitoring.web_dashboard.ConsumerGroupCoordinator") as mock_coord, \
             patch("src.monitoring.web_dashboard.SmartProducer") as mock_prod, \
             patch("src.monitoring.web_dashboard.LogGenerator"):

            mock_coord_instance = MagicMock()
            mock_coord_instance.active_consumers = 3
            mock_coord.return_value = mock_coord_instance

            mock_prod_instance = MagicMock()
            mock_prod_instance.stats = {"produced": 0, "errors": 0, "per_partition": {}}
            mock_prod.return_value = mock_prod_instance

            from src.monitoring.web_dashboard import create_app
            app = create_app(settings)

            with TestClient(app) as client:
                yield client

    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

    def test_stats_returns_json(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_consumed" in data
        assert "per_partition" in data

    def test_partitions_returns_json(self, client):
        resp = client.get("/api/partitions")
        assert resp.status_code == 200
        data = resp.json()
        assert "per_partition" in data
        assert "num_partitions" in data

    def test_dashboard_page_loads(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Kafka Partitioning" in resp.text
