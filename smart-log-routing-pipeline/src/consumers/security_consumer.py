"""Consumer for security log messages."""

import time

from colorama import Fore, Style, init

from src.consumers.base_consumer import BaseConsumer

init(autoreset=True)


class SecurityConsumer(BaseConsumer):
    """Processes messages from the security_logs queue."""

    def __init__(self, config=None):
        super().__init__(queue_name="security_logs", config=config)

    def process(self, message):
        """Print security details and simulate threat analysis."""
        print(
            f"  {Fore.YELLOW}\U0001f512 SECURITY PROCESSING: "
            f"{message['message']} \u2014 Analyzing for threats"
            f"{Style.RESET_ALL}"
        )
        time.sleep(0.01)
