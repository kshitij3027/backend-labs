"""Tests for the QueueManager class."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from src.queue_manager import QueueManager


class TestQueueManager:
    """Tests for QueueManager queue statistics methods."""

    @patch("src.queue_manager.requests.get")
    def test_get_queue_stats(self, mock_get, mock_config):
        """get_queue_stats should return correct structure for each queue."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "log_messages",
            "messages": 5,
            "consumers": 2,
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        manager = QueueManager(config=mock_config)
        stats = manager.get_queue_stats()

        assert len(stats) == 3
        for s in stats:
            assert "name" in s
            assert "messages" in s
            assert "consumers" in s

        assert stats[0]["name"] == "log_messages"
        assert stats[0]["messages"] == 5
        assert stats[0]["consumers"] == 2

    @patch("src.queue_manager.requests.get")
    def test_stats_api_url(self, mock_get, mock_config):
        """Management API URL should use host, management_port, and vhost-encoded queue name."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "log_messages",
            "messages": 0,
            "consumers": 0,
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        manager = QueueManager(config=mock_config)
        manager.get_queue_stats()

        # Check the first call's URL
        first_call = mock_get.call_args_list[0]
        url = first_call[0][0]
        assert url == "http://localhost:15672/api/queues/%2f/log_messages"

    @patch("src.queue_manager.requests.get")
    def test_stats_auth(self, mock_get, mock_config):
        """Management API requests should use basic auth with configured credentials."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "name": "log_messages",
            "messages": 0,
            "consumers": 0,
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        manager = QueueManager(config=mock_config)
        manager.get_queue_stats()

        first_call = mock_get.call_args_list[0]
        assert first_call[1]["auth"] == ("guest", "guest")

    @patch("src.queue_manager.requests.get")
    def test_stats_error_handling(self, mock_get, mock_config):
        """When the Management API fails, stats should return N/A values gracefully."""
        mock_get.side_effect = requests.RequestException("Connection refused")

        manager = QueueManager(config=mock_config)
        stats = manager.get_queue_stats()

        assert len(stats) == 3
        for s in stats:
            assert s["messages"] == "N/A"
            assert s["consumers"] == "N/A"

    @patch("src.queue_manager.LogPublisher")
    def test_publish_delegates_to_publisher(self, mock_pub_cls, mock_config):
        """publish should delegate to LogPublisher.publish."""
        mock_pub = MagicMock()
        mock_pub_cls.return_value = mock_pub

        manager = QueueManager(config=mock_config)
        manager.publish("info", "web", "test message")

        mock_pub.publish.assert_called_once_with("info", "web", "test message")
