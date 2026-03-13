"""Consumer for database log messages."""

import time

from colorama import Fore, Style, init

from src.consumers.base_consumer import BaseConsumer

init(autoreset=True)


class DatabaseConsumer(BaseConsumer):
    """Processes messages from the database_logs queue."""

    def __init__(self, config=None):
        super().__init__(queue_name="database_logs", config=config)

    def process(self, message):
        """Print database details and simulate performance analysis."""
        print(
            f"  {Fore.CYAN}\U0001f4be DATABASE PROCESSING: "
            f"{message['message']} \u2014 Performance analysis"
            f"{Style.RESET_ALL}"
        )
        time.sleep(0.01)
