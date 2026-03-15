"""Tests for the PriorityAgingMonitor."""

import time

import pytest

from src.aging import PriorityAgingMonitor
from src.config import Settings
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue


@pytest.fixture
def aging_settings() -> Settings:
    """Settings tuned for fast aging tests."""
    return Settings(
        max_queue_size=1000,
        aging_threshold_seconds=0.1,
        aging_check_interval=0.05,
    )


@pytest.fixture
def aging_queue(aging_settings) -> ThreadSafePriorityQueue:
    return ThreadSafePriorityQueue(
        max_size=aging_settings.max_queue_size,
        settings=aging_settings,
    )


class TestAgingPromotion:
    """Verify that stale messages are promoted."""

    def test_aging_promotes_message(self):
        # Use a longer threshold relative to the check interval so that only
        # one promotion can fire within the wait window.
        settings = Settings(
            max_queue_size=1000,
            aging_threshold_seconds=0.2,
            aging_check_interval=0.05,
        )
        queue = ThreadSafePriorityQueue(
            max_size=settings.max_queue_size,
            settings=settings,
        )
        queue.push(LogMessage(priority=Priority.LOW, message="stale"))

        monitor = PriorityAgingMonitor(queue, settings)
        monitor.start()

        try:
            # Wait for threshold (0.2s) + one check cycle (0.05s) + margin
            time.sleep(0.35)
        finally:
            monitor.stop()

        msg = queue.pop()
        assert msg is not None
        assert msg.priority == Priority.MEDIUM  # promoted from LOW once

    def test_aging_does_not_exceed_critical(self, aging_settings):
        # Use very aggressive settings so multiple promotions can fire
        aging_settings.aging_threshold_seconds = 0.05
        aging_settings.aging_check_interval = 0.03

        queue = ThreadSafePriorityQueue(
            max_size=aging_settings.max_queue_size,
            settings=aging_settings,
        )
        queue.push(LogMessage(priority=Priority.HIGH, message="will-age"))

        monitor = PriorityAgingMonitor(queue, aging_settings)
        monitor.start()

        try:
            # Wait long enough for potential multiple promotions
            time.sleep(0.5)
        finally:
            monitor.stop()

        msg = queue.pop()
        assert msg is not None
        assert msg.priority == Priority.CRITICAL

    def test_aging_no_promotion_before_threshold(self):
        settings = Settings(
            max_queue_size=1000,
            aging_threshold_seconds=10.0,  # very long threshold
            aging_check_interval=0.05,
        )
        queue = ThreadSafePriorityQueue(
            max_size=settings.max_queue_size,
            settings=settings,
        )
        queue.push(LogMessage(priority=Priority.LOW, message="fresh"))

        monitor = PriorityAgingMonitor(queue, settings)
        monitor.start()

        try:
            time.sleep(0.2)  # well below the 10s threshold
        finally:
            monitor.stop()

        msg = queue.pop()
        assert msg is not None
        assert msg.priority == Priority.LOW  # unchanged


class TestAgingLifecycle:
    """Verify start / stop."""

    def test_aging_stop(self, aging_queue, aging_settings):
        monitor = PriorityAgingMonitor(aging_queue, aging_settings)
        monitor.start()
        assert monitor.is_running

        monitor.stop()
        assert not monitor.is_running

    def test_aging_is_not_running_before_start(self, aging_queue, aging_settings):
        monitor = PriorityAgingMonitor(aging_queue, aging_settings)
        assert not monitor.is_running
