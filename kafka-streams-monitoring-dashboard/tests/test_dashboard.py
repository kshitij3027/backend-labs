"""Tests for src.dashboard module."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.dashboard import create_app, start_background_tasks


class TestHealthEndpoint:
    """Verify the /health endpoint."""

    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_healthy_status(self, client):
        response = client.get("/health")
        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert data["service"] == "kafka-streams-monitoring-dashboard"


class TestIndexEndpoint:
    """Verify the / endpoint."""

    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_index_contains_title(self, client):
        response = client.get("/")
        assert b"Kafka Streams Monitoring Dashboard" in response.data


class TestApiMetricsEndpoint:
    """Verify the /api/metrics endpoint."""

    def test_api_metrics_returns_json(self, client):
        response = client.get("/api/metrics")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "total_events" in data
        assert "per_topic_counts" in data
        assert "error_rate" in data
        assert "avg_response_time" in data
        assert "p95_response_time" in data
        assert "events_per_second" in data

    def test_api_metrics_returns_503_without_store(self, config):
        """When no metrics_store is provided, the endpoint returns 503."""
        app, _ = create_app(config)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/metrics")
            assert response.status_code == 503
            data = response.get_json()
            assert "error" in data


class TestApiHistoricalEndpoint:
    """Verify the /api/historical endpoint."""

    def test_api_historical_returns_json(self, client):
        response = client.get("/api/historical")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "labels" in data
        assert "events" in data
        assert "error_rate" in data
        assert "response_times" in data

    def test_api_historical_returns_503_without_store(self, config):
        app, _ = create_app(config)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/historical")
            assert response.status_code == 503


class TestApiAlertsEndpoint:
    """Verify the /api/alerts endpoint."""

    def test_api_alerts_returns_json(self, client):
        response = client.get("/api/alerts")
        assert response.status_code == 200
        data = response.get_json()
        assert "active" in data
        assert "history" in data

    def test_api_alerts_without_alert_manager(self, config, metrics_store):
        """When no alert_manager is provided, returns empty lists."""
        app, _ = create_app(config, metrics_store=metrics_store)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/alerts")
            assert response.status_code == 200
            data = response.get_json()
            assert data["active"] == []
            assert data["history"] == []


class TestApiBusinessMetricsEndpoint:
    """Verify the /api/business-metrics endpoint."""

    def test_api_business_metrics_returns_json(self, client):
        response = client.get("/api/business-metrics")
        assert response.status_code == 200
        data = response.get_json()
        assert "api_versions" in data
        assert "funnel" in data
        assert "auth" in data

    def test_api_business_metrics_without_tracker(self, config, metrics_store):
        """When no business_metrics tracker is provided, returns empty defaults."""
        app, _ = create_app(config, metrics_store=metrics_store)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/business-metrics")
            assert response.status_code == 200
            data = response.get_json()
            assert data["api_versions"] == {}
            assert data["funnel"] == {}
            assert data["auth"] == {}


class TestApiGeoEndpoint:
    """Verify the /api/geo endpoint."""

    def test_api_geo_returns_json(self, client):
        response = client.get("/api/geo")
        assert response.status_code == 200
        data = response.get_json()
        assert "traffic_by_region" in data
        assert "latency_by_region" in data

    def test_api_geo_without_analyzer(self, config, metrics_store):
        """When no geo_analyzer is provided, returns empty defaults."""
        app, _ = create_app(config, metrics_store=metrics_store)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/geo")
            assert response.status_code == 200
            data = response.get_json()
            assert data["traffic_by_region"] == {}
            assert data["latency_by_region"] == {}


class TestApiMetricsWithData:
    """Verify API responses with actual data in the store."""

    def test_metrics_reflect_added_events(self, client, metrics_store):
        now = time.time()
        metrics_store.add_event("log-events", {"response_time": 100, "timestamp": now})
        metrics_store.add_event("error-events", {"timestamp": now})

        response = client.get("/api/metrics")
        data = response.get_json()
        assert data["total_events"] == 2
        assert data["per_topic_counts"]["log-events"] == 1
        assert data["per_topic_counts"]["error-events"] == 1

    def test_business_metrics_reflect_tracking(self, client, business_metrics):
        business_metrics.track_api_version("/api/v1/test")
        business_metrics.track_auth_event("login", success=True)

        response = client.get("/api/business-metrics")
        data = response.get_json()
        assert data["api_versions"]["v1"] == 1
        assert data["auth"]["success"] == 1

    def test_geo_reflects_analysis(self, client, geo_analyzer):
        geo_analyzer.analyze_ip("8.8.8.8")
        geo_analyzer.record_latency("8.8.8.8", 150.0)

        response = client.get("/api/geo")
        data = response.get_json()
        assert sum(data["traffic_by_region"].values()) >= 1
        assert len(data["latency_by_region"]) >= 1


class TestApiAlertsWithData:
    """Verify alerts API when alerts are present."""

    def test_alerts_returns_active_and_history(self, config, metrics_store):
        from src.alerts import AlertManager

        am = AlertManager(config)
        am._cooldown_seconds = 0
        # Trigger a critical alert
        am.evaluate({"error_rate": 6.0, "p95_response_time": 200})

        app, _ = create_app(config, metrics_store=metrics_store, alert_manager=am)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.get("/api/alerts")
            data = response.get_json()
            assert len(data["active"]) >= 1
            assert len(data["history"]) >= 1
            assert data["active"][0]["severity"] == "critical"


class TestStartBackgroundTasks:
    """Verify start_background_tasks calls socketio.start_background_task."""

    def test_start_background_tasks_called(self, flask_app):
        mock_socketio = MagicMock()
        start_background_tasks(mock_socketio, flask_app, producer=None)
        mock_socketio.start_background_task.assert_called_once()

    def test_start_background_tasks_with_producer(self, flask_app):
        mock_socketio = MagicMock()
        mock_producer = MagicMock()
        start_background_tasks(mock_socketio, flask_app, producer=mock_producer)
        mock_socketio.start_background_task.assert_called_once()
