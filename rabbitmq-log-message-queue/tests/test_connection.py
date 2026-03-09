"""Tests for the RabbitMQConnection class (all mocked, no real RabbitMQ needed)."""

from unittest.mock import MagicMock, patch, call

import pika.exceptions
import pytest

from src.connection import RabbitMQConnection


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_connection_params(mock_creds, mock_params, mock_conn, mock_config):
    """Verify pika.ConnectionParameters is called with the correct arguments."""
    conn = RabbitMQConnection(mock_config)
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
def test_reconnect_on_failure(mock_creds, mock_params, mock_conn, mock_sleep, mock_config):
    """Verify reconnect retries on failure and eventually succeeds."""
    mock_conn.side_effect = [
        pika.exceptions.AMQPConnectionError("fail"),
        pika.exceptions.AMQPConnectionError("fail"),
        MagicMock(),  # third attempt succeeds
    ]

    conn = RabbitMQConnection(mock_config)
    result = conn.reconnect()

    assert result is not None
    assert mock_conn.call_count == 3
    # Exponential backoff: delay * 2^0 = 1, delay * 2^1 = 2
    assert mock_sleep.call_args_list == [call(1), call(2)]


@patch("src.connection.time.sleep")
@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_max_retries_exceeded(mock_creds, mock_params, mock_conn, mock_sleep, mock_config):
    """Verify AMQPConnectionError is raised after max retries are exhausted."""
    mock_conn.side_effect = pika.exceptions.AMQPConnectionError("always fails")

    conn = RabbitMQConnection(mock_config)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        conn.reconnect()

    assert mock_conn.call_count == mock_config.retry_max


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_context_manager(mock_creds, mock_params, mock_conn, mock_config):
    """Verify context manager connects on enter and closes on exit."""
    mock_connection_instance = MagicMock()
    mock_conn.return_value = mock_connection_instance
    mock_connection_instance.is_closed = False

    conn = RabbitMQConnection(mock_config)

    with conn as c:
        assert c is conn
        mock_conn.assert_called_once()

    mock_connection_instance.close.assert_called_once()


@patch("src.connection.pika.BlockingConnection")
@patch("src.connection.pika.ConnectionParameters")
@patch("src.connection.pika.PlainCredentials")
def test_close_connection(mock_creds, mock_params, mock_conn, mock_config):
    """Verify close() calls connection.close()."""
    mock_connection_instance = MagicMock()
    mock_conn.return_value = mock_connection_instance
    mock_connection_instance.is_closed = False

    conn = RabbitMQConnection(mock_config)
    conn.connect()
    conn.close()

    mock_connection_instance.close.assert_called_once()
    assert conn._connection is None
    assert conn._channel is None
