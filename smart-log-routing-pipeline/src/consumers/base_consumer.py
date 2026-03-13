"""Base consumer ABC for specialized log queue consumers."""

import json
import time
from abc import ABC, abstractmethod

from colorama import Fore, Style, init

from src.config import Config
from src.connection import RabbitMQConnection

init(autoreset=True)


class BaseConsumer(ABC):
    """Abstract base class for log consumers with manual acknowledgment."""

    def __init__(self, queue_name, config=None):
        self.queue_name = queue_name
        self._config = config or Config()
        self._conn_manager = RabbitMQConnection(self._config)
        self._channel = None
        self.stats = {
            "processed": 0,
            "errors": 0,
            "start_time": time.time(),
        }

    def connect(self):
        """Establish connection, obtain a channel, and set QoS prefetch."""
        self._conn_manager.connect()
        self._channel = self._conn_manager.get_channel()
        self._channel.basic_qos(prefetch_count=10)

    def start_consuming(self):
        """Declare the queue, set up the consumer callback, and block."""
        self._channel.queue_declare(queue=self.queue_name, durable=True)
        self._channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._on_message,
            auto_ack=False,
        )
        print(
            f"{Fore.GREEN}[*] Waiting for messages on '{self.queue_name}'. "
            f"Press Ctrl+C to exit.{Style.RESET_ALL}"
        )
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            self._channel.stop_consuming()
            print(
                f"\n{Fore.YELLOW}Consumer for '{self.queue_name}' "
                f"stopped gracefully.{Style.RESET_ALL}"
            )

    def _on_message(self, channel, method, properties, body):
        """Parse the JSON body, delegate to process(), and ack on success."""
        try:
            message = json.loads(body)
            self.process(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.stats["processed"] += 1
        except Exception as exc:
            self.stats["errors"] += 1
            print(
                f"{Fore.RED}Error processing message: {exc}{Style.RESET_ALL}"
            )
            # Negative-ack without requeue to avoid infinite loops
            channel.basic_nack(
                delivery_tag=method.delivery_tag, requeue=False
            )

    @abstractmethod
    def process(self, message):
        """Process a single log message dict. Subclasses must implement."""

    def get_stats(self):
        """Return a dict of consumer statistics."""
        uptime = time.time() - self.stats["start_time"]
        rate = (
            self.stats["processed"] / uptime if uptime > 0 else 0.0
        )
        return {
            "processed": self.stats["processed"],
            "errors": self.stats["errors"],
            "uptime": round(uptime, 2),
            "messages_per_sec": round(rate, 2),
        }

    def close(self):
        """Close the underlying connection."""
        self._conn_manager.close()
        self._channel = None
