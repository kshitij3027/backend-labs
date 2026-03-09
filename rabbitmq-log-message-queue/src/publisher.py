"""Log message publisher for RabbitMQ."""

import json
from datetime import datetime, timezone

import click
import pika
from rich.console import Console

from src.config import Config
from src.connection import RabbitMQConnection

console = Console()


class LogPublisher:
    """Publishes log messages to the RabbitMQ exchange with topic routing."""

    def __init__(self, config=None):
        self._config = config or Config()

    def publish(self, level, source, message):
        """Publish a single log message to the exchange.

        Args:
            level: Log level (info, error, debug).
            source: Source system identifier (e.g. web, api).
            message: The log message text.
        """
        routing_key = f"logs.{level}.{source}"
        body = json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "source": source,
            "message": message,
        })

        exchange_name = self._config.get_exchange_config()["name"]

        properties = pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        )

        conn = RabbitMQConnection(self._config)
        try:
            conn.connect()
            channel = conn.get_channel()
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=body,
                properties=properties,
            )
        finally:
            conn.close()

    def publish_batch(self, messages):
        """Publish multiple log messages.

        Args:
            messages: List of dicts with keys: level, source, message.
        """
        exchange_name = self._config.get_exchange_config()["name"]

        conn = RabbitMQConnection(self._config)
        try:
            conn.connect()
            channel = conn.get_channel()

            for msg in messages:
                routing_key = f"logs.{msg['level']}.{msg['source']}"
                body = json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": msg["level"],
                    "source": msg["source"],
                    "message": msg["message"],
                })

                properties = pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                )

                channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                )
        finally:
            conn.close()


@click.command()
@click.option("--level", type=click.Choice(["info", "error", "debug"]), required=True)
@click.option("--source", required=True)
@click.option("--message", "-m", required=True)
def main(level, source, message):
    """Publish a log message to RabbitMQ."""
    publisher = LogPublisher()
    publisher.publish(level, source, message)
    console.print(
        f"[green]\u2713[/green] Published [{level}] from {source}: {message}"
    )


if __name__ == "__main__":
    main()
