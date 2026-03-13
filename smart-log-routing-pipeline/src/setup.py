"""RabbitMQ exchange and queue topology setup for smart log routing pipeline."""

from colorama import Fore, Style, init

from src.config import Config
from src.connection import RabbitMQConnection

init(autoreset=True)


class RabbitMQSetup:
    """Declares exchanges, queues, and bindings from configuration."""

    def __init__(self, config=None):
        self._config = config or Config()

    def setup_exchanges(self, channel):
        """Declare all exchanges defined in the configuration.

        Args:
            channel: An open pika channel.
        """
        exchanges = self._config.get_exchange_configs()
        for exchange in exchanges:
            channel.exchange_declare(
                exchange=exchange["name"],
                exchange_type=exchange["type"],
                durable=True,
            )
            print(
                f"{Fore.GREEN}\u2713{Style.RESET_ALL} "
                f"Exchange declared: {exchange['name']} (type={exchange['type']})"
            )

    def setup_queues(self, channel):
        """Declare all queues and bind them to their configured exchanges.

        Args:
            channel: An open pika channel.
        """
        queues = self._config.get_queue_configs()
        for queue in queues:
            channel.queue_declare(
                queue=queue["name"],
                durable=True,
            )
            print(
                f"{Fore.GREEN}\u2713{Style.RESET_ALL} "
                f"Queue declared: {queue['name']}"
            )
            channel.queue_bind(
                queue=queue["name"],
                exchange=queue["exchange"],
                routing_key=queue["routing_key"],
            )
            print(
                f"{Fore.GREEN}\u2713{Style.RESET_ALL} "
                f"Queue {queue['name']} bound to {queue['exchange']} "
                f"with routing_key='{queue['routing_key']}'"
            )

    def setup_all(self):
        """Create connection, declare all exchanges and queues, then close."""
        conn = RabbitMQConnection(self._config)
        try:
            conn.connect()
            channel = conn.get_channel()

            print(f"\n{Fore.CYAN}=== Setting up exchanges ==={Style.RESET_ALL}")
            self.setup_exchanges(channel)

            print(f"\n{Fore.CYAN}=== Setting up queues ==={Style.RESET_ALL}")
            self.setup_queues(channel)

            exchanges = self._config.get_exchange_configs()
            queues = self._config.get_queue_configs()
            print(
                f"\n{Fore.GREEN}Setup complete: "
                f"{len(exchanges)} exchanges, {len(queues)} queues{Style.RESET_ALL}"
            )
        finally:
            conn.close()


if __name__ == "__main__":
    setup = RabbitMQSetup()
    setup.setup_all()
