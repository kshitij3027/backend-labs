"""Consumer for error log messages."""

import time

from colorama import Fore, Style, init

from src.consumers.base_consumer import BaseConsumer

init(autoreset=True)


class ErrorConsumer(BaseConsumer):
    """Processes messages from the error_logs queue."""

    def __init__(self, config=None):
        super().__init__(queue_name="error_logs", config=config)

    def process(self, message):
        """Print error details and simulate incident management dispatch."""
        print(
            f"  {Fore.RED}\U0001f6a8 ERROR PROCESSING: "
            f"{message['message']} \u2014 Sending to incident management"
            f"{Style.RESET_ALL}"
        )
        time.sleep(0.01)
