"""Tests for the WorkerPool."""

import time

import pytest

from src.models import LogMessage, Priority
from src.worker_pool import WorkerPool


class TestWorkerProcessing:
    """Verify that workers drain the queue and record metrics."""

    def test_worker_processes_messages(self, priority_queue, metrics, fast_settings):
        for _ in range(5):
            priority_queue.push(LogMessage(priority=Priority.LOW, message="test"))

        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=2)

        try:
            deadline = time.monotonic() + 2
            while not priority_queue.is_empty and time.monotonic() < deadline:
                time.sleep(0.01)

            assert priority_queue.is_empty
            stats = metrics.get_stats()
            assert stats["totals"]["processed"] == 5
        finally:
            pool.stop()

    def test_priority_order_processing(self, priority_queue, metrics, fast_settings):
        # Push in reverse priority order so the queue must re-order them
        priority_queue.push(LogMessage(priority=Priority.LOW, message="low"))
        priority_queue.push(LogMessage(priority=Priority.MEDIUM, message="med"))
        priority_queue.push(LogMessage(priority=Priority.HIGH, message="high"))
        priority_queue.push(LogMessage(priority=Priority.CRITICAL, message="crit"))

        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=1)

        try:
            deadline = time.monotonic() + 2
            while not priority_queue.is_empty and time.monotonic() < deadline:
                time.sleep(0.01)

            recent = metrics.get_recent_messages()
            assert len(recent) == 4
            # First message processed should be CRITICAL
            assert recent[0]["priority"] == "CRITICAL"
            # Second should be HIGH
            assert recent[1]["priority"] == "HIGH"
        finally:
            pool.stop()


class TestScaling:
    """Verify dynamic worker scaling."""

    def test_scale_up(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=2)

        try:
            pool.scale_to(4)
            time.sleep(0.1)
            assert pool.worker_count >= 4
        finally:
            pool.stop()

    def test_scale_down(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=4)

        try:
            time.sleep(0.05)
            assert pool.worker_count == 4

            pool.scale_to(2)
            # Give excess workers time to notice their stop event
            time.sleep(0.2)
            assert pool.worker_count <= 2
        finally:
            pool.stop()

    def test_scale_respects_min_workers(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=4)

        try:
            pool.scale_to(0)  # below min_workers (1)
            time.sleep(0.2)
            assert pool.worker_count >= fast_settings.min_workers
        finally:
            pool.stop()

    def test_scale_respects_max_workers(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=2)

        try:
            pool.scale_to(100)  # above max_workers (8)
            time.sleep(0.1)
            assert pool.worker_count <= fast_settings.max_workers
        finally:
            pool.stop()


class TestLifecycle:
    """Verify start / stop behaviour."""

    def test_stop_graceful(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=3)

        time.sleep(0.05)
        assert pool.is_running

        pool.stop()

        assert not pool.is_running
        for t in pool._workers:
            assert not t.is_alive()

    def test_worker_count_property(self, priority_queue, metrics, fast_settings):
        pool = WorkerPool(priority_queue, metrics, fast_settings)
        pool.start(num_workers=3)

        try:
            time.sleep(0.05)
            assert pool.worker_count == 3
        finally:
            pool.stop()
