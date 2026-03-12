"""Simulated log processor with configurable failure and timeout rates."""

import random

from src.logging_config import get_logger

logger = get_logger(__name__)


class ProcessingError(Exception):
    """Retryable processing error."""


class FatalProcessingError(Exception):
    """Non-retryable fatal error -- message should go directly to DLQ."""


class LogProcessor:
    """Simulates log message processing with probabilistic failures.

    Used to exercise the ack/retry/DLQ pipeline during development and
    testing.  In a production system this would be replaced by real
    processing logic.
    """

    def __init__(
        self,
        failure_rate: float = 0.2,
        timeout_rate: float = 0.1,
    ) -> None:
        self.failure_rate = failure_rate
        self.timeout_rate = timeout_rate

    def process(self, message: dict) -> dict:
        """Process a single log message.

        Raises:
            FatalProcessingError: When the message carries ``fatal=True``.
            ProcessingError: Randomly, based on *timeout_rate* and
                *failure_rate*.

        Returns:
            A dict with ``status`` and ``msg_id`` on success.
        """
        msg_id = message.get("id", "unknown")

        # Check fatal flag first (deterministic, before random failures)
        if message.get("fatal", False):
            logger.warning("fatal_message_detected", msg_id=msg_id)
            raise FatalProcessingError("fatal_message")

        # Simulate a processing timeout
        if random.random() < self.timeout_rate:
            logger.warning("processing_timeout", msg_id=msg_id)
            raise ProcessingError("processing_timeout")

        # Simulate a transient failure
        if random.random() < self.failure_rate:
            logger.warning("simulated_failure", msg_id=msg_id)
            raise ProcessingError("simulated_failure")

        # Happy path
        result = {"status": "processed", "msg_id": msg_id}
        logger.info("message_processed", msg_id=msg_id)
        return result
