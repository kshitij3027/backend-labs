"""Tests for the metrics collector module."""

import threading
import time

import pytest

from src.metrics import MetricsCollector


def test_record_and_snapshot():
    """Record 3 batches with different sizes/bytes/times, verify totals."""
    mc = MetricsCollector()

    mc.record_batch(batch_size=10, bytes_sent=500, send_time_ms=12.0, trigger="size")
    mc.record_batch(batch_size=20, bytes_sent=1000, send_time_ms=25.0, trigger="timer")
    mc.record_batch(batch_size=15, bytes_sent=750, send_time_ms=18.0, trigger="size")

    snap = mc.snapshot()

    assert snap["batches_sent"] == 3
    assert snap["total_entries"] == 45  # 10 + 20 + 15
    assert snap["total_bytes"] == 2250  # 500 + 1000 + 750
    assert snap["avg_batch_size"] == pytest.approx(15.0)
    assert snap["avg_send_time_ms"] == pytest.approx(55.0 / 3)
    assert snap["flush_triggers"]["size"] == 2
    assert snap["flush_triggers"]["timer"] == 1


def test_empty_snapshot():
    """Snapshot without any records should return all zeros."""
    mc = MetricsCollector()
    snap = mc.snapshot()

    assert snap["batches_sent"] == 0
    assert snap["total_entries"] == 0
    assert snap["total_bytes"] == 0
    assert snap["avg_batch_size"] == 0.0
    assert snap["p50_batch_size"] == 0
    assert snap["p95_batch_size"] == 0
    assert snap["avg_send_time_ms"] == 0.0
    assert snap["p95_send_time_ms"] == 0.0
    assert snap["flush_triggers"] == {"size": 0, "timer": 0}
    assert snap["uptime_seconds"] >= 0


def test_percentile_calculation():
    """Record many batches with known sizes, verify p50 and p95."""
    mc = MetricsCollector()

    # Record batches with sizes 1 through 100
    for i in range(1, 101):
        mc.record_batch(batch_size=i, bytes_sent=i * 10, send_time_ms=float(i))

    snap = mc.snapshot()

    # p50 of 1..100: index = 0.50 * 99 = 49.5 → interpolate between 50 and 51 → 50.5
    assert snap["p50_batch_size"] == pytest.approx(50.5)

    # p95 of 1..100: index = 0.95 * 99 = 94.05 → interpolate between 95 and 96
    # 95 + 0.05 * (96 - 95) = 95.05
    assert snap["p95_batch_size"] == pytest.approx(95.05)


def test_flush_trigger_ratio():
    """Record some 'size' and some 'timer' triggers, verify counts."""
    mc = MetricsCollector()

    for _ in range(7):
        mc.record_batch(batch_size=5, bytes_sent=100, send_time_ms=1.0, trigger="size")
    for _ in range(3):
        mc.record_batch(batch_size=5, bytes_sent=100, send_time_ms=1.0, trigger="timer")

    snap = mc.snapshot()
    assert snap["flush_triggers"]["size"] == 7
    assert snap["flush_triggers"]["timer"] == 3
    assert snap["batches_sent"] == 10


def test_concurrent_thread_safety():
    """Spawn 10 threads each recording 100 batches, verify total_entries."""
    mc = MetricsCollector()
    num_threads = 10
    batches_per_thread = 100
    batch_size = 5

    barrier = threading.Barrier(num_threads)

    def worker():
        barrier.wait()
        for _ in range(batches_per_thread):
            mc.record_batch(
                batch_size=batch_size,
                bytes_sent=50,
                send_time_ms=1.0,
                trigger="size",
            )

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = mc.snapshot()
    expected_entries = num_threads * batches_per_thread * batch_size
    assert snap["total_entries"] == expected_entries
    assert snap["batches_sent"] == num_threads * batches_per_thread


def test_uptime_seconds():
    """Create collector, sleep briefly, verify uptime > 0."""
    mc = MetricsCollector()
    time.sleep(0.05)
    snap = mc.snapshot()
    assert snap["uptime_seconds"] >= 0.04
