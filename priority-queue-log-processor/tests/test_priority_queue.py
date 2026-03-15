"""Tests for the thread-safe priority queue."""

import threading

import pytest

from src.config import Settings
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue


class TestBasicOperations:
    def test_push_pop_single(self, priority_queue: ThreadSafePriorityQueue):
        msg = LogMessage(priority=Priority.HIGH, message="hello")
        assert priority_queue.push(msg) is True
        popped = priority_queue.pop()
        assert popped is not None
        assert popped.id == msg.id

    def test_empty_pop_returns_none(self, priority_queue: ThreadSafePriorityQueue):
        assert priority_queue.pop() is None

    def test_is_empty(self, priority_queue: ThreadSafePriorityQueue):
        assert priority_queue.is_empty is True
        priority_queue.push(LogMessage(message="x"))
        assert priority_queue.is_empty is False
        priority_queue.pop()
        assert priority_queue.is_empty is True

    def test_is_full(self):
        pq = ThreadSafePriorityQueue(max_size=2)
        pq.push(LogMessage(priority=Priority.CRITICAL, message="a"))
        pq.push(LogMessage(priority=Priority.CRITICAL, message="b"))
        assert pq.is_full is True


class TestPriorityOrdering:
    def test_priority_ordering(self, priority_queue: ThreadSafePriorityQueue):
        priority_queue.push(LogMessage(priority=Priority.LOW, message="low"))
        priority_queue.push(LogMessage(priority=Priority.CRITICAL, message="crit"))
        priority_queue.push(LogMessage(priority=Priority.HIGH, message="high"))
        priority_queue.push(LogMessage(priority=Priority.MEDIUM, message="med"))

        assert priority_queue.pop().priority == Priority.CRITICAL
        assert priority_queue.pop().priority == Priority.HIGH
        assert priority_queue.pop().priority == Priority.MEDIUM
        assert priority_queue.pop().priority == Priority.LOW

    def test_fifo_within_priority(self, priority_queue: ThreadSafePriorityQueue):
        msgs = [
            LogMessage(priority=Priority.MEDIUM, message=f"msg-{i}")
            for i in range(3)
        ]
        for m in msgs:
            priority_queue.push(m)

        for m in msgs:
            popped = priority_queue.pop()
            assert popped.id == m.id


class TestCapacityAndBackpressure:
    def test_max_size_rejection(self):
        pq = ThreadSafePriorityQueue(max_size=3)
        for _ in range(3):
            assert pq.push(LogMessage(priority=Priority.CRITICAL, message="fill")) is True
        assert pq.push(LogMessage(priority=Priority.CRITICAL, message="overflow")) is False

    def test_backpressure_low_watermark(self):
        """At 81% capacity, LOW is rejected but CRITICAL/HIGH/MEDIUM pass."""
        settings = Settings(max_queue_size=100)
        pq = ThreadSafePriorityQueue(max_size=100, settings=settings)

        # Fill to 81 items (81% utilization)
        for _ in range(81):
            pq.push(LogMessage(priority=Priority.CRITICAL, message="fill"))

        assert pq.push(LogMessage(priority=Priority.LOW, message="reject")) is False
        assert pq.push(LogMessage(priority=Priority.MEDIUM, message="ok")) is True
        assert pq.push(LogMessage(priority=Priority.HIGH, message="ok")) is True
        assert pq.push(LogMessage(priority=Priority.CRITICAL, message="ok")) is True

    def test_backpressure_medium_watermark(self):
        """At 91% capacity, LOW and MEDIUM are rejected; CRITICAL/HIGH pass."""
        settings = Settings(max_queue_size=100)
        pq = ThreadSafePriorityQueue(max_size=100, settings=settings)

        for _ in range(91):
            pq.push(LogMessage(priority=Priority.CRITICAL, message="fill"))

        assert pq.push(LogMessage(priority=Priority.LOW, message="no")) is False
        assert pq.push(LogMessage(priority=Priority.MEDIUM, message="no")) is False
        assert pq.push(LogMessage(priority=Priority.HIGH, message="ok")) is True
        assert pq.push(LogMessage(priority=Priority.CRITICAL, message="ok")) is True

    def test_backpressure_high_watermark(self):
        """At 96% capacity, only CRITICAL is accepted."""
        settings = Settings(max_queue_size=100)
        pq = ThreadSafePriorityQueue(max_size=100, settings=settings)

        for _ in range(96):
            pq.push(LogMessage(priority=Priority.CRITICAL, message="fill"))

        assert pq.push(LogMessage(priority=Priority.LOW, message="no")) is False
        assert pq.push(LogMessage(priority=Priority.MEDIUM, message="no")) is False
        assert pq.push(LogMessage(priority=Priority.HIGH, message="no")) is False
        assert pq.push(LogMessage(priority=Priority.CRITICAL, message="ok")) is True


class TestPromotion:
    def test_promote(self, priority_queue: ThreadSafePriorityQueue):
        msg = LogMessage(priority=Priority.LOW, message="promote me")
        priority_queue.push(msg)
        assert priority_queue.promote(msg.id, Priority.HIGH) is True

        popped = priority_queue.pop()
        assert popped.priority == Priority.HIGH
        assert popped.original_priority == Priority.LOW

    def test_promote_nonexistent(self, priority_queue: ThreadSafePriorityQueue):
        assert priority_queue.promote("no-such-id", Priority.CRITICAL) is False


class TestMetrics:
    def test_size_tracking(self, priority_queue: ThreadSafePriorityQueue):
        assert priority_queue.size == 0
        priority_queue.push(LogMessage(message="a"))
        priority_queue.push(LogMessage(message="b"))
        assert priority_queue.size == 2
        priority_queue.pop()
        assert priority_queue.size == 1

    def test_priority_counts(self, priority_queue: ThreadSafePriorityQueue):
        priority_queue.push(LogMessage(priority=Priority.CRITICAL, message="c1"))
        priority_queue.push(LogMessage(priority=Priority.CRITICAL, message="c2"))
        priority_queue.push(LogMessage(priority=Priority.LOW, message="l1"))

        counts = priority_queue.priority_counts
        assert counts[Priority.CRITICAL] == 2
        assert counts[Priority.LOW] == 1
        assert counts[Priority.HIGH] == 0

    def test_utilization(self):
        pq = ThreadSafePriorityQueue(max_size=10)
        assert pq.utilization == 0.0
        for _ in range(5):
            pq.push(LogMessage(priority=Priority.CRITICAL, message="x"))
        assert pq.utilization == pytest.approx(0.5)

    def test_get_stats_shape(self, priority_queue: ThreadSafePriorityQueue):
        stats = priority_queue.get_stats()
        assert "size" in stats
        assert "max_size" in stats
        assert "utilization" in stats
        assert "priority_counts" in stats
        assert "is_full" in stats
        assert set(stats["priority_counts"].keys()) == {"CRITICAL", "HIGH", "MEDIUM", "LOW"}


class TestConcurrency:
    def test_concurrent_push_pop(self):
        pq = ThreadSafePriorityQueue(max_size=5000)
        pushed = {"count": 0}
        popped = {"count": 0}
        push_lock = threading.Lock()
        pop_lock = threading.Lock()

        def pusher():
            local_count = 0
            for _ in range(100):
                if pq.push(LogMessage(priority=Priority.MEDIUM, message="concurrent")):
                    local_count += 1
            with push_lock:
                pushed["count"] += local_count

        def popper():
            local_count = 0
            for _ in range(200):
                if pq.pop() is not None:
                    local_count += 1
            with pop_lock:
                popped["count"] += local_count

        push_threads = [threading.Thread(target=pusher) for _ in range(10)]
        pop_threads = [threading.Thread(target=popper) for _ in range(5)]

        for t in push_threads + pop_threads:
            t.start()
        for t in push_threads + pop_threads:
            t.join()

        expected_remaining = pushed["count"] - popped["count"]
        assert pq.size == expected_remaining
        assert pq.size >= 0
