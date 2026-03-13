"""Log producer that publishes messages to RabbitMQ exchanges."""

import time

import pika
from colorama import Fore, Style, init

from src.config import Config
from src.connection import RabbitMQConnection
from src.models.log_message import LogMessage

init(autoreset=True)


class LogProducer:
    """Publishes log messages to direct, topic, and fanout exchanges."""

    def __init__(self, config=None):
        self._config = config or Config()
        self._conn_manager = RabbitMQConnection(self._config)
        self._channel = None
        self.message_count = 0

    def connect(self):
        """Establish connection and obtain a channel."""
        self._conn_manager.connect()
        self._channel = self._conn_manager.get_channel()

    def publish_to_direct(self, message: LogMessage):
        """Publish to logs_direct exchange using the log level as routing key.

        Args:
            message: LogMessage to publish.
        """
        routing_key = message.level
        self._channel.basic_publish(
            exchange="logs_direct",
            routing_key=routing_key,
            body=message.to_json(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        self.message_count += 1
        print(
            f"  {Fore.GREEN}\U0001f3af Direct: {routing_key}{Style.RESET_ALL}"
        )

    def publish_to_topic(self, message: LogMessage):
        """Publish to logs_topic exchange using the full hierarchical routing key.

        Args:
            message: LogMessage to publish.
        """
        routing_key = message.routing_key
        self._channel.basic_publish(
            exchange="logs_topic",
            routing_key=routing_key,
            body=message.to_json(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        self.message_count += 1
        print(
            f"  {Fore.CYAN}\U0001f3f7\ufe0f  Topic: {routing_key}{Style.RESET_ALL}"
        )

    def publish_to_fanout(self, message: LogMessage):
        """Publish to logs_fanout exchange (routing key is ignored).

        Args:
            message: LogMessage to publish.
        """
        self._channel.basic_publish(
            exchange="logs_fanout",
            routing_key="",
            body=message.to_json(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        self.message_count += 1
        print(
            f"  {Fore.YELLOW}\U0001f4e2 Fanout: {message.message[:50]}{Style.RESET_ALL}"
        )

    def publish_to_all(self, message: LogMessage):
        """Publish the message to all three exchanges."""
        self.publish_to_direct(message)
        self.publish_to_topic(message)
        self.publish_to_fanout(message)

    def run_continuous(self, rate=10):
        """Generate and publish random log messages continuously.

        Args:
            rate: Messages per second (sleep = 1/rate between iterations).
        """
        print(f"\n{Fore.CYAN}=== Starting continuous producer (rate={rate}/s) ==={Style.RESET_ALL}")
        try:
            while True:
                message = LogMessage.generate_random()
                print(
                    f"\n{Fore.WHITE}[{message.timestamp}] "
                    f"{message.service}.{message.component} "
                    f"({message.level}): {message.message}{Style.RESET_ALL}"
                )
                self.publish_to_all(message)
                time.sleep(1 / rate)
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Producer interrupted.{Style.RESET_ALL}")
        finally:
            print(
                f"\n{Fore.CYAN}=== Producer stats ==={Style.RESET_ALL}\n"
                f"  Total messages published: {self.message_count}"
            )

    def close(self):
        """Close the underlying connection."""
        self._conn_manager.close()
        self._channel = None
