"""RabbitMQ exchange, queue, and dead-letter setup."""

from rich.console import Console

from src.config import Config
from src.connection import RabbitMQConnection

console = Console()


class RabbitMQSetup:
    """Declares exchanges, queues, and dead-letter infrastructure on RabbitMQ."""

    def __init__(self, config=None):
        self._config = config or Config()

    def setup_dlx(self, channel):
        """Create the dead-letter exchange and its queue."""
        dlx = self._config.get_dlx_config()

        channel.exchange_declare(
            exchange=dlx["exchange"],
            exchange_type="direct",
            durable=True,
        )

        channel.queue_declare(queue=dlx["queue"], durable=True)

        channel.queue_bind(
            queue=dlx["queue"],
            exchange=dlx["exchange"],
            routing_key=dlx["routing_key"],
        )

    def setup_exchange(self, channel):
        """Create the main log exchange."""
        exc = self._config.get_exchange_config()

        channel.exchange_declare(
            exchange=exc["name"],
            exchange_type=exc["type"],
            durable=exc["durable"],
        )

    def setup_queues(self, channel):
        """Declare all queues and bind them to the main exchange."""
        exc = self._config.get_exchange_config()
        dlx = self._config.get_dlx_config()
        queues = self._config.get_queue_configs()

        dlx_args = {
            "x-dead-letter-exchange": dlx["exchange"],
            "x-dead-letter-routing-key": dlx["routing_key"],
        }

        for q in queues:
            channel.queue_declare(
                queue=q["name"],
                durable=q["durable"],
                arguments=dlx_args,
            )

            channel.queue_bind(
                queue=q["name"],
                exchange=exc["name"],
                routing_key=q["routing_key"],
            )

    def setup_all(self):
        """Create the full RabbitMQ topology (DLX, exchange, queues)."""
        conn = RabbitMQConnection(self._config)

        try:
            conn.connect()
            channel = conn.get_channel()

            console.print("[bold cyan]Setting up RabbitMQ topology...[/bold cyan]")

            self.setup_dlx(channel)
            console.print("[green]\u2713[/green] Dead-letter exchange and queue created")

            self.setup_exchange(channel)
            console.print("[green]\u2713[/green] Main exchange created")

            self.setup_queues(channel)
            console.print("[green]\u2713[/green] Queues declared and bound")

            console.print("[bold green]\u2713 RabbitMQ topology setup complete![/bold green]")
        finally:
            conn.close()


if __name__ == "__main__":
    setup = RabbitMQSetup()
    setup.setup_all()
