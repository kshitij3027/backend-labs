"""Consumer for audit log messages."""

import time

from colorama import Fore, Style, init

from src.consumers.base_consumer import BaseConsumer

init(autoreset=True)


class AuditConsumer(BaseConsumer):
    """Processes messages from the audit_logs queue."""

    def __init__(self, config=None):
        super().__init__(queue_name="audit_logs", config=config)

    def process(self, message):
        """Print audit recording details."""
        print(
            f"  {Fore.WHITE}\U0001f4cb AUDIT RECORDING: "
            f"{message['message']}"
            f"{Style.RESET_ALL}"
        )
        time.sleep(0.01)
