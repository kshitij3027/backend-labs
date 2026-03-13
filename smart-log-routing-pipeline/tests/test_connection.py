"""Tests for the RabbitMQConnection class (all mocked, no real RabbitMQ needed)."""

from unittest.mock import MagicMock, patch, call

import pika.exceptions
import pytest

from src.connection import RabbitMQConnection


def _make_mock_config():
    """Create a mock config with standard test values."""
    config = MagicMock()
    config.retry_max = 5
    config.retry_delay = 2
    config.get_connection_params.return_value = {
        "host": "localhost",
        "port": 5672,
        "credentials": {
            "username": "guest",
            "password": "guest",
        },
        "heartbeat": 600,
        "blocked_connection_timeout": 300,
    }
    return config


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_connect_creates_connection(mock_creds, mock_params, mock_conn):
    """Verify pika.BlockingConnection is called with correct params."""
    config = _make_mock_config()
    conn = RabbitMQConnection(config)
    conn.connect()

    mock_creds.assert_called_once_with("guest", "guest")
    mock_params.assert_called_once_with(
        host="localhost",
        port=5672,
        credentials=mock_creds.return_value,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    mock_conn.assert_called_once_with(mock_params.return_value)


@patch("src.connection.time.sleep")
@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_reconnect_retries_on_failure(mock_creds, mock_params, mock_conn, mock_sleep):
    """Verify reconnect retries on failure and eventually succeeds."""
    mock_conn.side_effect = [
        pika.exceptions.AMQPConnectionError("fail"),
        pika.exceptions.AMQPConnectionError("fail"),
        MagicMock(),  # third attempt succeeds
    ]

    config = _make_mock_config()
    conn = RabbitMQConnection(config)
    result = conn.reconnect()

    assert result is not None
    assert mock_conn.call_count == 3
    # Exponential backoff: delay * 2^0 = 2, delay * 2^1 = 4
    assert mock_sleep.call_args_list == [call(2), call(4)]


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_get_channel(mock_creds, mock_params, mock_conn):
    """Verify get_channel returns a channel from the connection."""
    mock_connection_instance = MagicMock()
    mock_connection_instance.is_closed = False
    mock_conn.return_value = mock_connection_instance

    mock_channel = MagicMock()
    mock_channel.is_closed = False
    mock_connection_instance.channel.return_value = mock_channel

    config = _make_mock_config()
    conn = RabbitMQConnection(config)
    conn.connect()

    channel = conn.get_channel()

    mock_connection_instance.channel.assert_called_once()
    assert channel is mock_channel


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_close(mock_creds, mock_params, mock_conn):
    """Verify close() calls connection.close() and clears state."""
    mock_connection_instance = MagicMock()
    mock_connection_instance.is_closed = False
    mock_conn.return_value = mock_connection_instance

    config = _make_mock_config()
    conn = RabbitMQConnection(config)
    conn.connect()
    conn.close()

    mock_connection_instance.close.assert_called_once()
    assert conn._connection is None
    assert conn._channel is None


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_context_manager(mock_creds, mock_params, mock_conn):
    """Verify __enter__ connects and __exit__ closes."""
    mock_connection_instance = MagicMock()
    mock_connection_instance.is_closed = False
    mock_conn.return_value = mock_connection_instance

    config = _make_mock_config()
    conn = RabbitMQConnection(config)

    with conn as c:
        assert c is conn
        mock_conn.assert_called_once()

    mock_connection_instance.close.assert_called_once()


@patch("src.connection.time.sleep")
@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_reconnect_raises_after_max_retries(mock_creds, mock_params, mock_conn, mock_sleep):
    """Verify AMQPConnectionError is raised after exhausting retries."""
    mock_conn.side_effect = pika.exceptions.AMQPConnectionError("always fails")

    config = _make_mock_config()
    conn = RabbitMQConnection(config)

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        conn.reconnect()

    assert mock_conn.call_count == config.retry_max
