"""Tests for the HealthChecker class."""

from unittest.mock import MagicMock, patch

import pytest

from src.health_checker import HealthChecker


class TestHealthChecker:
    """Tests for HealthChecker health-check methods (mocked, no real RabbitMQ)."""

    @patch("src.health_checker.RabbitMQConnection")
    def test_healthy_connection(self, mock_conn_cls, mock_config):
        """check_connection should return healthy when connect succeeds."""
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn

        checker = HealthChecker(config=mock_config)
        result = checker.check_connection()

        assert result["status"] == "healthy"
        assert result["message"] == "Connection successful"
        mock_conn.connect.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("src.health_checker.RabbitMQConnection")
    def test_unhealthy_connection(self, mock_conn_cls, mock_config):
        """check_connection should return unhealthy when connect raises."""
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.connect.side_effect = Exception("Connection refused")

        checker = HealthChecker(config=mock_config)
        result = checker.check_connection()

        assert result["status"] == "unhealthy"
        assert "Connection failed" in result["message"]
        mock_conn.close.assert_called_once()

    @patch("src.health_checker.requests.get")
    def test_management_api_healthy(self, mock_get, mock_config):
        """check_management_api should return healthy with version on 200."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"rabbitmq_version": "3.13.1"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        checker = HealthChecker(config=mock_config)
        result = checker.check_management_api()

        assert result["status"] == "healthy"
        assert "3.13.1" in result["message"]

    @patch("src.health_checker.requests.get")
    def test_management_api_unhealthy(self, mock_get, mock_config):
        """check_management_api should return unhealthy when request fails."""
        mock_get.side_effect = Exception("Connection refused")

        checker = HealthChecker(config=mock_config)
        result = checker.check_management_api()

        assert result["status"] == "unhealthy"
        assert "unreachable" in result["message"].lower() or "Management API" in result["message"]

    @patch("src.health_checker.requests.get")
    def test_queues_healthy(self, mock_get, mock_config):
        """check_queues should return healthy when all queues return 200."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        checker = HealthChecker(config=mock_config)
        result = checker.check_queues()

        assert result["status"] == "healthy"
        assert result["message"] == "All queues operational"
        assert mock_get.call_count == 3

    @patch("src.health_checker.requests.get")
    def test_queues_missing(self, mock_get, mock_config):
        """check_queues should return unhealthy when a queue returns 404."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            if "error_messages" in url:
                resp.status_code = 404
            else:
                resp.status_code = 200
            return resp

        mock_get.side_effect = side_effect

        checker = HealthChecker(config=mock_config)
        result = checker.check_queues()

        assert result["status"] == "unhealthy"
        assert "error_messages" in result["message"]

    @patch.object(HealthChecker, "check_queues")
    @patch.object(HealthChecker, "check_management_api")
    @patch.object(HealthChecker, "check_connection")
    def test_overall_healthy(self, mock_conn, mock_api, mock_queues, mock_config):
        """run_health_check should return overall healthy when all checks pass."""
        mock_conn.return_value = {"status": "healthy", "message": "Connection successful"}
        mock_api.return_value = {"status": "healthy", "message": "Running (Version: 3.13.1)"}
        mock_queues.return_value = {"status": "healthy", "message": "All queues operational"}

        checker = HealthChecker(config=mock_config)
        report = checker.run_health_check()

        assert report["overall"] == "healthy"
        assert report["connection"]["status"] == "healthy"
        assert report["management_api"]["status"] == "healthy"
        assert report["queues"]["status"] == "healthy"

    @patch.object(HealthChecker, "check_queues")
    @patch.object(HealthChecker, "check_management_api")
    @patch.object(HealthChecker, "check_connection")
    def test_overall_unhealthy(self, mock_conn, mock_api, mock_queues, mock_config):
        """run_health_check should return overall unhealthy when any check fails."""
        mock_conn.return_value = {"status": "healthy", "message": "Connection successful"}
        mock_api.return_value = {"status": "unhealthy", "message": "Management API unreachable: Connection refused"}
        mock_queues.return_value = {"status": "healthy", "message": "All queues operational"}

        checker = HealthChecker(config=mock_config)
        report = checker.run_health_check()

        assert report["overall"] == "unhealthy"
        assert report["connection"]["status"] == "healthy"
        assert report["management_api"]["status"] == "unhealthy"
        assert report["queues"]["status"] == "healthy"
