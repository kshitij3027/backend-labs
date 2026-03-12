"""Shared pytest fixtures for the log-consumer-ack-redelivery test suite."""

import pytest

from src.ack_tracker import AckTracker
from src.config import Settings


@pytest.fixture
def config() -> Settings:
    """Return a Settings instance with test-friendly defaults."""
    return Settings(
        RABBITMQ_HOST="localhost",
        RABBITMQ_PORT=5672,
        RABBITMQ_USER="guest",
        RABBITMQ_PASS="guest",
        RETRY_DELAYS=[1000, 2000, 4000, 8000],
        MAX_RETRIES=5,
        ACK_TIMEOUT_SEC=30,
    )


@pytest.fixture
def ack_tracker() -> AckTracker:
    """Return a fresh AckTracker instance."""
    return AckTracker()
