"""Shared pytest fixtures."""

import pytest

from src.aging import PriorityAgingMonitor
from src.classifier import MessageClassifier
from src.config import Settings
from src.metrics import MetricsTracker
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue
from src.worker_pool import WorkerPool


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def small_settings() -> Settings:
    return Settings(max_queue_size=100)


@pytest.fixture
def fast_settings() -> Settings:
    return Settings(
        max_queue_size=1000,
        num_workers=2,
        critical_process_time_ms=1,
        high_process_time_ms=1,
        medium_process_time_ms=1,
        low_process_time_ms=1,
        aging_threshold_seconds=0.1,
        aging_check_interval=0.05,
        min_workers=1,
        max_workers=8,
    )


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


@pytest.fixture
def worker_pool(priority_queue, metrics, fast_settings):
    pool = WorkerPool(priority_queue, metrics, fast_settings)
    yield pool
    pool.stop()


@pytest.fixture
def aging_monitor(priority_queue, fast_settings):
    monitor = PriorityAgingMonitor(priority_queue, fast_settings)
    yield monitor
    monitor.stop()
