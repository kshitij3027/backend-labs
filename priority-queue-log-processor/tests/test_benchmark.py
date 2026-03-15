"""Performance benchmark tests for the priority queue log processor."""

import random
import threading
import time
import tracemalloc

import pytest

from src.config import Settings
from src.metrics import MetricsTracker
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue
from src.worker_pool import WorkerPool


@pytest.fixture
def benchmark_settings():
    return Settings(
        max_queue_size=50000,
        num_workers=8,
        critical_process_time_ms=1,
        high_process_time_ms=2,
        medium_process_time_ms=3,
        low_process_time_ms=5,
        min_workers=2,
        max_workers=16,
    )


class TestThroughput:
    """Verify the system can sustain target throughput."""

    @pytest.mark.timeout(60)
    def test_throughput_1000_msgs_per_second(self, benchmark_settings):
        queue = ThreadSafePriorityQueue(
            max_size=50000, settings=benchmark_settings
        )
        metrics = MetricsTracker()
        pool = WorkerPool(queue, metrics, benchmark_settings)

        priorities = list(Priority)
        messages = [
            LogMessage(
                priority=random.choice(priorities),
                source="bench",
                message=f"benchmark message {i}",
            )
            for i in range(10000)
        ]

        try:
            pool.start(num_workers=8)

            start = time.monotonic()

            for msg in messages:
                pushed = queue.push(msg)
                assert pushed, "Queue should accept all messages (capacity 50000)"

            # Wait for all messages to be processed (check metrics, not just
            # queue size, because a worker may have popped but not yet recorded)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                stats = metrics.get_stats()
                if stats["totals"]["processed"] >= 10000:
                    break
                time.sleep(0.1)

            elapsed = time.monotonic() - start

            stats = metrics.get_stats()
            processed = stats["totals"]["processed"]
            throughput = processed / elapsed

            assert processed == 10000, (
                f"Expected 10000 processed, got {processed}"
            )
            assert throughput >= 1000, (
                f"Throughput {throughput:.0f} msg/s is below 1000 msg/s target"
            )
        finally:
            pool.stop()


class TestLatency:
    """Verify processing latency meets SLA targets."""

    @pytest.mark.timeout(30)
    def test_critical_latency_under_50ms(self, benchmark_settings):
        queue = ThreadSafePriorityQueue(
            max_size=50000, settings=benchmark_settings
        )
        metrics = MetricsTracker()
        pool = WorkerPool(queue, metrics, benchmark_settings)

        try:
            pool.start(num_workers=4)

            for i in range(100):
                msg = LogMessage(
                    priority=Priority.CRITICAL,
                    source="bench",
                    message=f"critical latency test {i}",
                    created_at=time.time(),
                )
                queue.push(msg)

            # Wait for all messages to be processed (check metrics count,
            # not queue size, to avoid race with in-flight processing)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                stats = metrics.get_stats()
                if stats["processed"]["CRITICAL"] >= 100:
                    break
                time.sleep(0.1)

            stats = metrics.get_stats()
            critical_times = stats["processing_times"]["CRITICAL"]
            p95_seconds = critical_times["p95"]
            p95_ms = p95_seconds * 1000

            assert stats["processed"]["CRITICAL"] == 100, (
                f"Expected 100 CRITICAL processed, got {stats['processed']['CRITICAL']}"
            )
            assert p95_ms < 50, (
                f"CRITICAL p95 latency {p95_ms:.1f}ms exceeds 50ms target"
            )
        finally:
            pool.stop()


class TestOrdering:
    """Verify strict priority ordering without workers."""

    def test_priority_ordering_under_load(self):
        queue = ThreadSafePriorityQueue(max_size=5000)

        # Create messages with known counts per priority
        critical_msgs = [
            LogMessage(
                priority=Priority.CRITICAL,
                source="bench",
                message=f"critical-{i}",
            )
            for i in range(50)
        ]
        high_msgs = [
            LogMessage(
                priority=Priority.HIGH,
                source="bench",
                message=f"high-{i}",
            )
            for i in range(150)
        ]
        medium_msgs = [
            LogMessage(
                priority=Priority.MEDIUM,
                source="bench",
                message=f"medium-{i}",
            )
            for i in range(300)
        ]
        low_msgs = [
            LogMessage(
                priority=Priority.LOW,
                source="bench",
                message=f"low-{i}",
            )
            for i in range(500)
        ]

        all_messages = critical_msgs + high_msgs + medium_msgs + low_msgs
        random.shuffle(all_messages)

        # Track insertion order per priority to verify FIFO later
        insertion_order: dict[str, list[str]] = {
            "CRITICAL": [],
            "HIGH": [],
            "MEDIUM": [],
            "LOW": [],
        }

        for msg in all_messages:
            pushed = queue.push(msg)
            assert pushed
            insertion_order[msg.priority.name].append(msg.id)

        assert queue.size == 1000

        # Pop all messages and verify strict priority ordering
        popped = []
        while not queue.is_empty:
            msg = queue.pop()
            assert msg is not None
            popped.append(msg)

        assert len(popped) == 1000

        # Verify all CRITICALs come before all HIGHs, etc.
        critical_indices = [i for i, m in enumerate(popped) if m.priority == Priority.CRITICAL]
        high_indices = [i for i, m in enumerate(popped) if m.priority == Priority.HIGH]
        medium_indices = [i for i, m in enumerate(popped) if m.priority == Priority.MEDIUM]
        low_indices = [i for i, m in enumerate(popped) if m.priority == Priority.LOW]

        assert len(critical_indices) == 50
        assert len(high_indices) == 150
        assert len(medium_indices) == 300
        assert len(low_indices) == 500

        # Strict ordering: max index of higher priority < min index of lower
        assert max(critical_indices) < min(high_indices), (
            "All CRITICAL messages must come before all HIGH messages"
        )
        assert max(high_indices) < min(medium_indices), (
            "All HIGH messages must come before all MEDIUM messages"
        )
        assert max(medium_indices) < min(low_indices), (
            "All MEDIUM messages must come before all LOW messages"
        )

        # Within same priority, verify FIFO order (by insertion/push order)
        for label, group_indices in [
            ("CRITICAL", critical_indices),
            ("HIGH", high_indices),
            ("MEDIUM", medium_indices),
            ("LOW", low_indices),
        ]:
            group_ids = [popped[i].id for i in group_indices]
            expected_ids = insertion_order[label]
            assert group_ids == expected_ids, (
                f"{label} messages are not in FIFO (insertion) order"
            )


class TestMemory:
    """Verify memory usage stays within bounds."""

    def test_memory_under_100mb(self):
        tracemalloc.start()

        try:
            queue = ThreadSafePriorityQueue(max_size=50000)

            priorities = list(Priority)
            for i in range(50000):
                msg = LogMessage(
                    priority=priorities[i % len(priorities)],
                    source="bench",
                    message=f"memory test message {i}",
                )
                queue.push(msg)

            _snapshot = tracemalloc.take_snapshot()
            _current, peak = tracemalloc.get_traced_memory()

            limit = 100 * 1024 * 1024  # 100 MB
            assert peak < limit, (
                f"Peak memory {peak / (1024 * 1024):.1f}MB exceeds 100MB limit"
            )
        finally:
            tracemalloc.stop()


class TestConcurrentStress:
    """Verify thread safety under heavy concurrent access."""

    @pytest.mark.timeout(60)
    def test_concurrent_stress(self):
        queue = ThreadSafePriorityQueue(max_size=100000)

        errors: list[Exception] = []
        pushed_count = {"value": 0}
        pushed_lock = threading.Lock()
        popped_count = {"value": 0}
        popped_lock = threading.Lock()
        stop_event = threading.Event()

        priorities = list(Priority)

        def producer(thread_id: int) -> None:
            try:
                for i in range(500):
                    msg = LogMessage(
                        priority=random.choice(priorities),
                        source=f"producer-{thread_id}",
                        message=f"stress-{thread_id}-{i}",
                    )
                    # Retry push if queue is full (should not happen with 100k capacity)
                    pushed = queue.push(msg)
                    if pushed:
                        with pushed_lock:
                            pushed_count["value"] += 1
            except Exception as e:
                errors.append(e)

        def consumer(thread_id: int) -> None:
            local_count = 0
            try:
                while not stop_event.is_set():
                    msg = queue.pop()
                    if msg is not None:
                        local_count += 1
                    else:
                        time.sleep(0.001)
                # Drain remaining after stop signal
                while True:
                    msg = queue.pop()
                    if msg is None:
                        break
                    local_count += 1
            except Exception as e:
                errors.append(e)
            finally:
                with popped_lock:
                    popped_count["value"] += local_count

        # Spawn producers
        producer_threads = [
            threading.Thread(target=producer, args=(i,), name=f"producer-{i}")
            for i in range(20)
        ]
        # Spawn consumers
        consumer_threads = [
            threading.Thread(target=consumer, args=(i,), name=f"consumer-{i}")
            for i in range(10)
        ]

        # Start all threads
        for t in consumer_threads:
            t.start()
        for t in producer_threads:
            t.start()

        # Wait for producers to finish
        for t in producer_threads:
            t.join(timeout=30)

        # Let consumers drain for a bit, then signal stop
        time.sleep(0.5)
        stop_event.set()

        for t in consumer_threads:
            t.join(timeout=10)

        # Verify no exceptions
        assert len(errors) == 0, f"Errors during stress test: {errors}"

        # Consistency: pushed - popped == remaining in queue
        remaining = queue.size
        total_p = pushed_count["value"]
        total_c = popped_count["value"]

        assert total_p == 10000, (
            f"Expected 10000 pushed, got {total_p}"
        )
        assert total_p - total_c == remaining, (
            f"Inconsistency: pushed={total_p}, popped={total_c}, "
            f"queue.size={remaining}, diff={total_p - total_c}"
        )


class TestBackpressure:
    """Verify backpressure rejects low-priority messages under load."""

    def test_backpressure_under_load(self):
        # Use default watermarks: low=0.8, medium=0.9, high=0.95
        queue = ThreadSafePriorityQueue(max_size=100)

        # Fill the queue using CRITICAL messages (bypass all watermarks)
        for i in range(100):
            msg = LogMessage(
                priority=Priority.CRITICAL,
                source="bench",
                message=f"fill-{i}",
            )
            queue.push(msg)

        assert queue.size == 100

        # Pop a few to bring utilization down to the backpressure zone
        # Pop 15 to get to 85/100 = 85% (above low watermark 80%)
        for _ in range(15):
            queue.pop()

        assert queue.size == 85

        # Try to push LOW messages -- should be rejected (above low watermark 80%)
        low_rejections = 0
        for i in range(50):
            msg = LogMessage(
                priority=Priority.LOW,
                source="bench",
                message=f"low-overflow-{i}",
            )
            if not queue.push(msg):
                low_rejections += 1

        assert low_rejections > 0, (
            "Backpressure should reject LOW messages when above low watermark"
        )

        # Try to push CRITICAL messages -- should be accepted until 100% full
        critical_accepted = 0
        for i in range(10):
            msg = LogMessage(
                priority=Priority.CRITICAL,
                source="bench",
                message=f"critical-overflow-{i}",
            )
            if queue.push(msg):
                critical_accepted += 1

        assert critical_accepted > 0, (
            "CRITICAL messages should bypass watermarks and be accepted"
        )
