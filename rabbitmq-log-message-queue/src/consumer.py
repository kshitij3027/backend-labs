"""Log message consumer for RabbitMQ."""

import json
import signal
import sys

import click
from rich.console import Console

from src.config import Config
from src.connection import RabbitMQConnection

console = Console()


class LogConsumer:
    """Consumes log messages from a RabbitMQ queue with manual acknowledgement."""

    def __init__(self, queue_name, config=None):
        self._queue_name = queue_name
        self._config = config or Config()
        self._connection = None
        self._channel = None
        self._consuming = False

    def _default_callback(self, log_entry):
        """Pretty-print a log entry using rich formatting."""
        level = log_entry.get("level", "unknown")
        source = log_entry.get("source", "unknown")
        timestamp = log_entry.get("timestamp", "")
        message = log_entry.get("message", "")

        level_colors = {
            "info": "green",
            "error": "red",
            "debug": "yellow",
        }
        color = level_colors.get(level, "white")

        console.print(
            f"[dim]{timestamp}[/dim] "
            f"[bold {color}][{level.upper()}][/bold {color}] "
            f"[cyan]{source}[/cyan]: {message}"
        )

    def consume(self, callback=None):
        """Start consuming messages from the configured queue.

        Args:
            callback: Optional callable that receives the parsed log dict.
                      If None, uses the default rich-formatted printer.
        """
        user_callback = callback or self._default_callback

        def _on_message(channel, method, properties, body):
            log_entry = json.loads(body)
            user_callback(log_entry)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        conn = RabbitMQConnection(self._config)
        self._connection = conn

        def _shutdown(signum, frame):
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        try:
            conn.connect()
            self._channel = conn.get_channel()
            self._channel.basic_qos(prefetch_count=1)

            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_on_message,
                auto_ack=False,
            )

            self._consuming = True
            console.print(
                f"[bold cyan]Consuming from queue: {self._queue_name}[/bold cyan]"
            )
            console.print("[dim]Press Ctrl+C to stop[/dim]")
            self._channel.start_consuming()
        finally:
            conn.close()

    def stop(self):
        """Stop the consumer gracefully."""
        self._consuming = False
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()
        if self._connection:
            self._connection.close()
        console.print("[yellow]Consumer stopped.[/yellow]")


@click.command()
@click.option(
    "--queue",
    type=click.Choice(["log_messages", "error_messages", "debug_messages"]),
    required=True,
)
def main(queue):
    """Consume log messages from a RabbitMQ queue."""
    consumer = LogConsumer(queue)
    consumer.consume()


if __name__ == "__main__":
    main()
