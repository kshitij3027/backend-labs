"""Shared pytest fixtures."""

import pytest

from src.classifier import MessageClassifier
from src.config import Settings
from src.metrics import MetricsTracker
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def small_settings() -> Settings:
    return Settings(max_queue_size=100)


@pytest.fixture
def priority_queue(small_settings: Settings) -> ThreadSafePriorityQueue:
    return ThreadSafePriorityQueue(
        max_size=small_settings.max_queue_size,
        settings=small_settings,
    )


@pytest.fixture
def sample_messages() -> list[LogMessage]:
    return [
        LogMessage(priority=Priority.CRITICAL, source="auth", message="Security breach detected"),
        LogMessage(priority=Priority.HIGH, source="api", message="High latency on /checkout"),
        LogMessage(priority=Priority.MEDIUM, source="web", message="User validation failed"),
        LogMessage(priority=Priority.LOW, source="cron", message="Scheduled cleanup finished"),
    ]


@pytest.fixture
def classifier() -> MessageClassifier:
    return MessageClassifier()


@pytest.fixture
def metrics() -> MetricsTracker:
    return MetricsTracker()
