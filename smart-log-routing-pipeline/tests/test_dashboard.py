"""Tests for the Flask + SocketIO dashboard and StatsCollector."""

import time
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.dashboard.app import app
from src.dashboard.stats_collector import StatsCollector


# ── Flask app tests ──────────────────────────────────────────────────

class TestFlaskApp:
    """Tests for the dashboard Flask application."""

    def test_flask_app_creation(self):
        """The app object should be a Flask instance."""
        assert isinstance(app, Flask)

    def test_health_endpoint(self):
        """GET /health should return 200 with status ok."""
        client = app.test_client()
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data == {"status": "ok"}

    def test_index_returns_html(self):
        """GET / should return 200 with HTML content."""
        client = app.test_client()
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.content_type

    @patch("src.dashboard.app.stats_collector")
    def test_api_stats_returns_json(self, mock_collector):
        """GET /api/stats should return 200 with a JSON list."""
        mock_collector.get_queue_stats.return_value = [
            {
                "name": "error_logs",
                "messages": 5,
                "messages_ready": 3,
                "consumers": 1,
                "message_stats": {"publish_rate": 1.0, "deliver_rate": 0.5},
            }
        ]
        client = app.test_client()
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
        assert data[0]["name"] == "error_logs"


# ── StatsCollector tests ─────────────────────────────────────────────

class TestStatsCollector:
    """Tests for the StatsCollector class."""

    @patch("src.dashboard.stats_collector.Config")
    def test_stats_collector_creation(self, MockConfig):
        """StatsCollector should build its base URL from the config."""
        cfg = MagicMock()
        cfg.host = "myhost"
        cfg.management_port = 15672
        cfg.username = "user"
        cfg.password = "pass"

        collector = StatsCollector(config=cfg)
        assert collector._base_url == "http://myhost:15672/api"
        assert collector._auth == ("user", "pass")

    @patch("src.dashboard.stats_collector.Config")
    @patch("src.dashboard.stats_collector.requests.get")
    def test_stats_collector_cache(self, mock_get, MockConfig):
        """Subsequent calls within the TTL should return cached data (single HTTP call)."""
        cfg = MagicMock()
        cfg.host = "localhost"
        cfg.management_port = 15672
        cfg.username = "guest"
        cfg.password = "guest"

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {
                "name": "test_q",
                "messages": 10,
                "messages_ready": 5,
                "consumers": 2,
                "message_stats": {
                    "publish_details": {"rate": 2.0},
                    "deliver_get_details": {"rate": 1.0},
                },
            }
        ]
        mock_get.return_value = mock_resp

        collector = StatsCollector(config=cfg)

        first = collector.get_queue_stats()
        second = collector.get_queue_stats()

        # Only one HTTP call should have been made thanks to caching
        assert mock_get.call_count == 1
        assert first == second
        assert first[0]["name"] == "test_q"
