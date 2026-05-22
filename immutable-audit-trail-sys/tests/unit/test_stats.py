"""Unit tests for the Counters class — thread safety + snapshot semantics."""
import threading

from src.stats.counters import Counters, get_counters, reset_counters_for_tests


def test_initial_snapshot_is_zero():
    c = Counters()
    snap = c.snapshot()
    assert snap == {
        "records_appended": 0,
        "verifications_run": 0,
        "integrity_breaks_detected": 0,
        "decorator_invocations_total": 0,
        "decorator_failures_total": 0,
    }


def test_increment_then_snapshot():
    c = Counters()
    c.incr_records_appended()
    c.incr_records_appended(3)
    c.incr_verifications_run()
    c.incr_integrity_breaks()
    c.incr_decorator_invocations(5)
    c.incr_decorator_failures()
    snap = c.snapshot()
    assert snap["records_appended"] == 4
    assert snap["verifications_run"] == 1
    assert snap["integrity_breaks_detected"] == 1
    assert snap["decorator_invocations_total"] == 5
    assert snap["decorator_failures_total"] == 1


def test_concurrent_increments_are_consistent():
    """100 threads x 100 increments = exactly 10_000."""
    c = Counters()
    def worker():
        for _ in range(100):
            c.incr_records_appended()
    threads = [threading.Thread(target=worker) for _ in range(100)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert c.snapshot()["records_appended"] == 10_000


def test_observe_decorator_overhead_does_not_crash():
    c = Counters()
    c.observe_decorator_overhead_ms(2.5)
    c.observe_decorator_overhead_ms(0.1)
    c.observe_decorator_overhead_ms(50.0)


def test_get_counters_returns_singleton():
    reset_counters_for_tests()
    a = get_counters()
    b = get_counters()
    assert a is b
    reset_counters_for_tests()
