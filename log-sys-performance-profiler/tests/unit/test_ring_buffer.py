import threading
import time

import pytest

from src.metrics.ring_buffer import RingBuffer
from src.metrics.sample import MetricSample


def _sample(stage="parse", ts=None, latency_ms=1.0):
    return MetricSample(
        stage=stage,
        ts=ts if ts is not None else time.time(),
        cpu_pct=10.0,
        mem_mb=50.0,
        io_read_bytes=0,
        io_write_bytes=0,
        queue_depth=0,
        latency_ms=latency_ms,
    )


def test_maxlen_eviction():
    rb = RingBuffer(maxlen=10)
    # Use distinct ts so we can verify oldest eviction
    for i in range(20):
        rb.add(_sample(ts=float(i)))
    items = rb.snapshot()
    assert len(rb) == 10
    assert len(items) == 10
    # Oldest entries (ts=0..9) evicted; first remaining should be ts=10
    assert items[0].ts == 10.0
    assert items[-1].ts == 19.0


def test_snapshot_returns_copy():
    rb = RingBuffer(maxlen=100)
    for i in range(3):
        rb.add(_sample(ts=float(i)))
    snap = rb.snapshot()
    original_len = len(snap)
    snap.append(_sample(ts=999.0))
    snap.clear()
    # Buffer untouched by mutation of returned list
    snap2 = rb.snapshot()
    assert len(snap2) == original_len
    assert [s.ts for s in snap2] == [0.0, 1.0, 2.0]


def test_snapshot_window_filter():
    rb = RingBuffer(maxlen=100)
    now = time.time()
    # Spread samples across ~20s; only ones within 5s should appear
    rb.add(_sample(ts=now - 20.0))
    rb.add(_sample(ts=now - 15.0))
    rb.add(_sample(ts=now - 10.0))
    rb.add(_sample(ts=now - 3.0))
    rb.add(_sample(ts=now - 1.0))
    recent = rb.snapshot(window_sec=5.0)
    assert len(recent) == 2
    assert all(s.ts >= now - 5.0 for s in recent)


def test_by_stage_filter():
    rb = RingBuffer(maxlen=100)
    now = time.time()
    for _ in range(3):
        rb.add(_sample(stage="parse", ts=now))
    for _ in range(2):
        rb.add(_sample(stage="validate", ts=now))
    parse_samples = rb.by_stage("parse", window_sec=60.0)
    validate_samples = rb.by_stage("validate", window_sec=60.0)
    assert len(parse_samples) == 3
    assert len(validate_samples) == 2
    assert all(s.stage == "parse" for s in parse_samples)


def test_clear():
    rb = RingBuffer(maxlen=10)
    for i in range(5):
        rb.add(_sample(ts=float(i)))
    assert len(rb) == 5
    rb.clear()
    assert len(rb) == 0
    assert rb.snapshot() == []


def test_empty_snapshot():
    rb = RingBuffer(maxlen=10)
    assert rb.snapshot() == []
    assert rb.snapshot(window_sec=5.0) == []
    assert len(rb) == 0


def test_thread_safety_smoke():
    rb = RingBuffer(maxlen=1000)
    errors: list[BaseException] = []

    def worker():
        try:
            for _ in range(500):
                rb.add(_sample())
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(rb) <= rb.maxlen
    # Both workers wrote a combined 1000 samples; deque maxlen is 1000 so all fit
    assert len(rb) == 1000


def test_zero_maxlen_raises():
    with pytest.raises(ValueError):
        RingBuffer(0)
    with pytest.raises(ValueError):
        RingBuffer(-5)
