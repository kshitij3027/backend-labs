"""RabbitMQ connection manager with reconnection and context manager support."""

import time

import pika
import pika.exceptions

from src.config import Config


class RabbitMQConnection:
    """Manages a connection to RabbitMQ with retry logic and context manager support."""

    def __init__(self, config: Config):
        self._config = config
        self._connection = None
        self._channel = None

    def connect(self):
        """Establish a blocking connection to RabbitMQ."""
        params = self._config.get_connection_params()
        credentials = pika.PlainCredentials(
            params["credentials"]["username"],
            params["credentials"]["password"],
        )
        connection_params = pika.ConnectionParameters(
            host=params["host"],
            port=params["port"],
            credentials=credentials,
            heartbeat=params["heartbeat"],
            blocked_connection_timeout=params["blocked_connection_timeout"],
        )
        self._connection = pika.BlockingConnection(connection_params)
        self._channel = None
        return self._connection

    def reconnect(self):
        """Reconnect with exponential backoff. Raises on max retries exceeded."""
        max_retries = self._config.retry_max
        delay = self._config.retry_delay

        for attempt in range(max_retries):
            try:
                return self.connect()
            except pika.exceptions.AMQPConnectionError:
                if attempt == max_retries - 1:
                    raise
                wait_time = delay * (2 ** attempt)
                time.sleep(wait_time)

    def get_channel(self):
        """Return a channel from the current connection, reconnecting if needed."""
        if self._connection is None or self._connection.is_closed:
            self.connect()
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        return self._channel

    def close(self):
        """Close the connection if open."""
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        self._connection = None
        self._channel = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
