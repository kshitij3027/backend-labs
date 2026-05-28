from __future__ import annotations

import time

from src.analysis.detector import BottleneckDetector
from src.metrics.ring_buffer import RingBuffer
from src.metrics.sample import MetricSample
from src.settings import Settings


def _s(stage, ts_offset=0.0, cpu=10.0, mem=50.0, q=10, lat=1.0):
    return MetricSample(
        stage=stage,
        ts=time.time() - ts_offset,
        cpu_pct=cpu,
        mem_mb=mem,
        io_read_bytes=0,
        io_write_bytes=0,
        queue_depth=q,
        latency_ms=lat,
    )


def _settings() -> Settings:
    return Settings(
        detection_window_sec=10,
        bottleneck_z_threshold=2.0,
        queue_maxsize=256,
        theoretical_max_lps=50000,
    )


def _buffer(samples) -> RingBuffer:
    buf = RingBuffer(maxlen=10_000)
    for s in samples:
        buf.add(s)
    return buf


def test_serial_classification():
    samples = []
    # Baseline samples (older — ts_offset 12-50s ago) for validate at ~10ms with variation.
    baseline_lats = [8.0, 9.0, 10.0, 11.0, 12.0, 9.5, 10.5, 11.5, 8.5, 12.5,
                     9.0, 10.0, 11.0, 8.0, 12.0, 10.0, 11.0, 9.0, 10.5, 9.5]
    for i, lat in enumerate(baseline_lats):
        # ts_offset between 12 and 50 to land inside (now-60, now-10) baseline window.
        samples.append(_s("validate", ts_offset=12.0 + i * 2.0, lat=lat))
    # Current-window samples: validate dominates at 80ms; others modest.
    for i in range(20):
        samples.append(_s("parse", ts_offset=5.0 + i * 0.1, lat=5.0))
    for i in range(20):
        samples.append(_s("validate", ts_offset=5.0 + i * 0.1, lat=80.0))
    for i in range(20):
        samples.append(_s("transform", ts_offset=5.0 + i * 0.1, lat=4.0))
    for i in range(20):
        samples.append(_s("write", ts_offset=5.0 + i * 0.1, lat=3.0))

    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    serial = [b for b in bottlenecks if b.type == "serial"]
    assert len(serial) >= 1
    assert serial[0].stage == "validate"
    assert serial[0].z_score >= 2.0


def test_resource_high_cpu_classification():
    samples = []
    for i in range(20):
        samples.append(_s("transform", ts_offset=5.0 + i * 0.1, cpu=95.0, lat=1.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    resource = [b for b in bottlenecks if b.type == "resource"]
    assert len(resource) >= 1
    assert any(b.stage == "transform" for b in resource)


def test_resource_mem_growth():
    samples = []
    # 20 write samples spanning 2 seconds, mem 50 -> 100 -> ~25 MB/s slope.
    for i in range(20):
        # Oldest first: ts_offset highest at i=0, lowest at i=19. Buffer preserves insertion order,
        # so stage_samples[0] is the oldest sample with the lowest mem.
        ts_offset = 2.0 - i * (2.0 / 19)  # 2.0 down to 0.0
        mem = 50.0 + i * (50.0 / 19)  # 50 up to 100
        samples.append(_s("write", ts_offset=ts_offset, cpu=10.0, mem=mem, lat=1.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    resource = [b for b in bottlenecks if b.type == "resource"]
    assert len(resource) >= 1
    assert any(b.stage == "write" for b in resource)


def test_contention_back_pressure():
    samples = []
    qmax = 256
    # 20 validate samples (B stage in parse->validate); 18 at maxsize, 2 mid.
    for i in range(20):
        q = qmax if i < 18 else 10
        samples.append(_s("validate", ts_offset=5.0 + i * 0.1, q=q, lat=1.0, cpu=10.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    contention = [b for b in bottlenecks if b.type == "contention"]
    assert len(contention) >= 1
    pair = next((b for b in contention if b.stage == "parse->validate"), None)
    assert pair is not None
    assert pair.details.get("kind") == "back_pressure"


def test_contention_starvation():
    samples = []
    # 20 validate samples with queue_depth=0 on 18; no other class firing.
    # Don't pass throughput_lps so architectural never evaluates.
    for i in range(20):
        q = 0 if i < 18 else 5
        samples.append(_s("validate", ts_offset=5.0 + i * 0.1, q=q, lat=1.0, cpu=10.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    contention = [b for b in bottlenecks if b.type == "contention"]
    pair = next((b for b in contention if b.stage == "parse->validate"), None)
    assert pair is not None
    assert pair.details.get("kind") == "starvation"


def test_architectural_fires_on_low_throughput():
    samples = []
    # 10 samples per stage, all healthy.
    for stage in ("parse", "validate", "transform", "write"):
        for i in range(10):
            samples.append(_s(stage, ts_offset=5.0 + i * 0.1, cpu=10.0, mem=50.0, q=10, lat=1.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate(throughput_lps=1000.0)

    arch = [b for b in bottlenecks if b.type == "architectural"]
    assert len(arch) == 1
    assert arch[0].stage == "all"


def test_transient_spike_below_z_suppressed():
    samples = []
    # 5 parse samples at lat=10 + 1 spike at lat=50; only parse stage has data.
    for i in range(5):
        samples.append(_s("parse", ts_offset=5.0 + i * 0.1, lat=10.0))
    samples.append(_s("parse", ts_offset=4.0, lat=50.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate()

    serial = [b for b in bottlenecks if b.type == "serial"]
    assert serial == []


def test_empty_buffer_returns_empty():
    buf = RingBuffer(maxlen=100)
    detector = BottleneckDetector(buf, _settings())
    assert detector.evaluate() == []


def test_all_healthy_returns_empty():
    samples = []
    for stage in ("parse", "validate", "transform", "write"):
        for i in range(20):
            samples.append(_s(stage, ts_offset=5.0 + i * 0.1, cpu=10.0, mem=50.0, q=10, lat=1.0))
    buf = _buffer(samples)
    detector = BottleneckDetector(buf, _settings())
    bottlenecks = detector.evaluate(throughput_lps=40000.0)
    assert bottlenecks == []
