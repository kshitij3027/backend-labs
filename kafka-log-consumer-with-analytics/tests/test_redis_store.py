"""Tests for Redis store."""
import json
from unittest.mock import MagicMock, patch

import pytest

from src.redis_store import RedisStore


class TestRedisStoreDisconnected:
    """Tests when Redis is unavailable."""

    @patch("src.redis_store.redis.Redis")
    def test_graceful_degradation(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.side_effect = ConnectionError("refused")
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        assert store.is_connected is False

    @patch("src.redis_store.redis.Redis")
    def test_save_when_disconnected(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.side_effect = ConnectionError("refused")
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        result = store.save_snapshot({"key": "value"})
        assert result is False

    @patch("src.redis_store.redis.Redis")
    def test_load_when_disconnected(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.side_effect = ConnectionError("refused")
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        result = store.load_snapshot()
        assert result is None


class TestRedisStoreConnected:
    """Tests with a mocked Redis connection."""

    @patch("src.redis_store.redis.Redis")
    def test_save_snapshot(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.return_value = True
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        assert store.is_connected is True

        data = {"total_messages": 100, "endpoints": {"api": {"count": 50}}}
        result = store.save_snapshot(data)
        assert result is True
        mock_instance.hset.assert_called_once()

    @patch("src.redis_store.redis.Redis")
    def test_load_snapshot(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.return_value = True
        mock_instance.hgetall.return_value = {
            "total_messages": "100",
            "endpoints": '{"api": {"count": 50}}',
        }
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        snapshot = store.load_snapshot()
        assert snapshot is not None
        assert snapshot["total_messages"] == 100
        assert snapshot["endpoints"] == {"api": {"count": 50}}

    @patch("src.redis_store.redis.Redis")
    def test_load_empty_snapshot(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.return_value = True
        mock_instance.hgetall.return_value = {}
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        assert store.load_snapshot() is None

    @patch("src.redis_store.redis.Redis")
    def test_ping(self, mock_redis_cls):
        mock_instance = MagicMock()
        mock_instance.ping.return_value = True
        mock_redis_cls.return_value = mock_instance

        store = RedisStore(host="localhost")
        assert store.ping() is True
