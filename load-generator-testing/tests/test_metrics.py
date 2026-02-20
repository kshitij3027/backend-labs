import pytest

from generator.metrics import Metrics


def test_empty_metrics():
    m = Metrics()
    m.start()
    m.stop()
    s = m.summary()
    assert s["total_sent"] == 0
    assert s["actual_rps"] == 0.0


def test_record_success():
    m = Metrics()
    m.start()
    m.record(1.5, True, 100)
    m.record(2.0, True, 100)
    m.stop()
    s = m.summary()
    assert s["total_sent"] == 2
    assert s["total_success"] == 2
    assert s["total_errors"] == 0
    assert s["error_rate"] == 0.0
    assert s["total_bytes"] == 200


def test_record_failure():
    m = Metrics()
    m.start()
    m.record(1.0, True, 50)
    m.record(5.0, False, 50)
    m.stop()
    s = m.summary()
    assert s["total_sent"] == 2
    assert s["total_success"] == 1
    assert s["total_errors"] == 1
    assert s["error_rate"] == 0.5


def test_percentiles():
    m = Metrics()
    m.start()
    # Add 100 latencies: 1, 2, 3, ..., 100
    for i in range(1, 101):
        m.record(float(i), True, 10)
    m.stop()
    s = m.summary()
    assert s["latency_p50_ms"] == pytest.approx(50.5, abs=1.0)
    assert s["latency_p95_ms"] == pytest.approx(95.5, abs=1.0)
    assert s["latency_p99_ms"] == pytest.approx(99.5, abs=1.0)
    assert s["latency_min_ms"] == 1.0
    assert s["latency_max_ms"] == 100.0


def test_actual_rps():
    m = Metrics()
    m.start()
    for _ in range(100):
        m.record(1.0, True, 10)
    m.stop()
    s = m.summary()
    assert s["actual_rps"] > 0
    assert s["duration_secs"] > 0
