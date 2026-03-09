"""Queue manager for publishing and monitoring RabbitMQ queues."""

import requests
import click
from rich.console import Console

from src.config import Config
from src.publisher import LogPublisher

console = Console()


class QueueManager:
    """High-level manager for publishing messages and querying queue statistics."""

    def __init__(self, config=None):
        self._config = config or Config()
        self._publisher = LogPublisher(self._config)

    def publish(self, level, source, message):
        """Publish a log message via the internal publisher.

        Args:
            level: Log level (info, error, debug).
            source: Source system identifier.
            message: The log message text.
        """
        self._publisher.publish(level, source, message)

    def get_queue_stats(self):
        """Fetch queue statistics from the RabbitMQ Management API.

        Returns:
            List of dicts with keys: name, messages, consumers.
        """
        queues = self._config.get_queue_configs()
        host = self._config.host
        mgmt_port = self._config.management_port
        username = self._config.username
        password = self._config.password

        stats = []
        for q in queues:
            url = f"http://{host}:{mgmt_port}/api/queues/%2f/{q['name']}"
            try:
                resp = requests.get(url, auth=(username, password), timeout=5)
                resp.raise_for_status()
                data = resp.json()
                stats.append({
                    "name": data.get("name", q["name"]),
                    "messages": data.get("messages", 0),
                    "consumers": data.get("consumers", 0),
                })
            except requests.RequestException:
                stats.append({
                    "name": q["name"],
                    "messages": "N/A",
                    "consumers": "N/A",
                })

        return stats

    def display_stats(self):
        """Print queue statistics with rich formatting."""
        stats = self.get_queue_stats()
        console.print("[bold cyan]Queue Statistics[/bold cyan]")
        for s in stats:
            console.print(
                f"\U0001f4ca Queue {s['name']}: "
                f"{s['messages']} messages, {s['consumers']} consumers"
            )


@click.group()
def cli():
    """RabbitMQ log queue manager."""
    pass


@cli.command()
@click.option("--level", type=click.Choice(["info", "error", "debug"]), required=True)
@click.option("--source", required=True)
@click.option("--message", "-m", required=True)
def publish(level, source, message):
    """Publish a log message."""
    manager = QueueManager()
    manager.publish(level, source, message)
    console.print(
        f"[green]\u2713[/green] Published [{level}] from {source}: {message}"
    )


@cli.command()
def stats():
    """Display queue statistics."""
    manager = QueueManager()
    manager.display_stats()


if __name__ == "__main__":
    cli()
