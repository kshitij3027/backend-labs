"""RabbitMQ connection management with retry logic."""

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


class RabbitMQConnection:
    """Manages a RabbitMQ connection with automatic retry on failure."""

    def __init__(self, config):
        self._config = config
        self._connection = None
        self._channel = None

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((AMQPConnectionError, AMQPChannelError)),
    )
    def connect(self):
        """Establish a connection to RabbitMQ with exponential backoff retry."""
        rmq = self._config.rabbitmq
        credentials = pika.PlainCredentials(rmq["user"], rmq["password"])
        params = pika.ConnectionParameters(
            host=rmq["host"],
            port=rmq["port"],
            credentials=credentials,
            heartbeat=rmq["heartbeat"],
            blocked_connection_timeout=rmq["blocked_connection_timeout"],
        )
        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        return self

    def get_channel(self):
        """Return the current channel."""
        return self._channel

    def close(self):
        """Close the connection safely."""
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
        except Exception:
            pass
        finally:
            self._connection = None
            self._channel = None

    @property
    def is_connected(self):
        """Check if the connection is open."""
        return (
            self._connection is not None
            and not self._connection.is_closed
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
