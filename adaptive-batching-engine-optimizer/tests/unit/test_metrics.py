"""Unit tests for src.metrics — ResourceMonitor (psutil mocked) and MetricsCollector.

Deterministic and isolated: psutil is always monkeypatched so no real host
counters are read, and there are no sleeps or network calls.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.metrics import MetricsCollector, ResourceMonitor, ResourceReading
from src.models import MetricSnapshot


# --- ResourceMonitor (psutil fully mocked) ----------------------------------


class _FakeCpu:
    """Records every call to cpu_percent and returns queued values in order."""

    def __init__(self, values: list[float]) -> None:
        self._values = list(values)
        self.calls: list[dict] = []

    def __call__(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        # Return the next queued value; fall back to the last once exhausted.
        if len(self.calls) <= len(self._values):
            return self._values[len(self.calls) - 1]
        return self._values[-1] if self._values else 0.0


def _patch_psutil(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cpu_values: list[float],
    mem_percent: float,
    mem_available_bytes: int,
) -> _FakeCpu:
    """Patch src.metrics.psutil.{cpu_percent,virtual_memory}; return the cpu spy."""
    fake_cpu = _FakeCpu(cpu_values)
    monkeypatch.setattr("src.metrics.psutil.cpu_percent", fake_cpu)
    monkeypatch.setattr(
        "src.metrics.psutil.virtual_memory",
        lambda: SimpleNamespace(percent=mem_percent, available=mem_available_bytes),
    )
    return fake_cpu


def test_resource_monitor_primes_cpu_on_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    """__init__ issues exactly one throwaway non-blocking cpu_percent call."""
    fake_cpu = _patch_psutil(
        monkeypatch, cpu_values=[0.0, 12.5], mem_percent=40.0, mem_available_bytes=0
    )

    ResourceMonitor()

    assert len(fake_cpu.calls) == 1  # primed once, before any sample()
    assert fake_cpu.calls[0]["kwargs"] == {"interval": None}


def test_resource_monitor_sample_returns_reading(monkeypatch: pytest.MonkeyPatch) -> None:
    # 1 GiB available => exactly 1024.0 MB after the bytes/1024**2 conversion.
    one_gib = 1024 * 1024 * 1024
    fake_cpu = _patch_psutil(
        monkeypatch,
        cpu_values=[0.0, 37.5],  # priming read then the real sample read
        mem_percent=63.0,
        mem_available_bytes=one_gib,
    )

    monitor = ResourceMonitor()
    reading = monitor.sample()

    assert isinstance(reading, ResourceReading)
    assert reading.cpu_percent == pytest.approx(37.5)
    assert reading.memory_percent == pytest.approx(63.0)
    assert reading.memory_available_mb == pytest.approx(1024.0)


def test_resource_monitor_available_mb_conversion(monkeypatch: pytest.MonkeyPatch) -> None:
    """available_mb is bytes / 1024**2 (mebibytes), not bytes / 1e6."""
    # 1,500,000,000 bytes -> 1430.51... MiB
    raw_bytes = 1_500_000_000
    _patch_psutil(
        monkeypatch,
        cpu_values=[0.0, 5.0],
        mem_percent=10.0,
        mem_available_bytes=raw_bytes,
    )

    reading = ResourceMonitor().sample()

    assert reading.memory_available_mb == pytest.approx(raw_bytes / (1024**2))


def test_resource_monitor_cpu_percent_always_non_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every cpu_percent call (prime + samples) is non-blocking: interval=None."""
    fake_cpu = _patch_psutil(
        monkeypatch,
        cpu_values=[0.0, 10.0, 20.0],
        mem_percent=50.0,
        mem_available_bytes=512 * 1024 * 1024,
    )

    monitor = ResourceMonitor()
    monitor.sample()
    monitor.sample()

    assert len(fake_cpu.calls) == 3  # 1 prime + 2 samples
    for call in fake_cpu.calls:
        assert call["kwargs"].get("interval", "MISSING") is None
        assert call["args"] == ()  # interval passed as a keyword, not positional


# --- MetricsCollector -------------------------------------------------------


def _record(collector: MetricsCollector, *, ts: float, throughput: float, batch: int = 100):
    """Record a snapshot via record_metrics with sensible filler fields."""
    return collector.record_metrics(
        timestamp=ts,
        batch_size=batch,
        throughput=throughput,
        latency_ms=10.0,
        cpu_percent=20.0,
        memory_percent=30.0,
        memory_available_mb=4096.0,
        queue_depth=0,
    )


def test_record_metrics_returns_and_stores_snapshot() -> None:
    collector = MetricsCollector()
    assert len(collector) == 0

    snap = collector.record_metrics(
        timestamp=1.0,
        batch_size=128,
        throughput=500.0,
        latency_ms=25.0,
        cpu_percent=40.0,
        memory_percent=55.0,
        memory_available_mb=2048.0,
        queue_depth=3,
    )

    assert isinstance(snap, MetricSnapshot)
    assert snap.batch_size == 128
    assert snap.throughput == pytest.approx(500.0)
    assert snap.queue_depth == 3

    assert len(collector) == 1
    assert collector.latest() is snap


def test_record_metrics_queue_depth_defaults_to_zero() -> None:
    collector = MetricsCollector()
    snap = collector.record_metrics(
        timestamp=1.0,
        batch_size=64,
        throughput=100.0,
        latency_ms=5.0,
        cpu_percent=10.0,
        memory_percent=20.0,
        memory_available_mb=1024.0,
    )
    assert snap.queue_depth == 0


def test_latest_returns_none_when_empty() -> None:
    assert MetricsCollector().latest() is None


def test_record_appends_prebuilt_snapshot() -> None:
    collector = MetricsCollector()
    snap = MetricSnapshot(
        timestamp=2.0,
        batch_size=200,
        throughput=750.0,
        latency_ms=12.0,
        cpu_percent=15.0,
        memory_percent=25.0,
        memory_available_mb=3072.0,
        queue_depth=9,
    )

    result = collector.record(snap)

    assert result is None  # record() returns nothing
    assert len(collector) == 1
    assert collector.latest() is snap


def test_len_increments_with_each_record() -> None:
    collector = MetricsCollector()
    for i in range(4):
        _record(collector, ts=float(i), throughput=float(i))
    assert len(collector) == 4


def test_snapshot_returns_all_oldest_to_newest() -> None:
    collector = MetricsCollector()
    for i in range(5):
        _record(collector, ts=float(i), throughput=float(i * 10))

    snaps = collector.snapshot()

    assert len(snaps) == 5
    # Oldest first, newest last.
    assert [s.timestamp for s in snaps] == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert [s.throughput for s in snaps] == [0.0, 10.0, 20.0, 30.0, 40.0]


def test_snapshot_last_n_returns_correct_slice() -> None:
    collector = MetricsCollector()
    for i in range(5):
        _record(collector, ts=float(i), throughput=float(i))

    last_two = collector.snapshot(2)

    assert [s.timestamp for s in last_two] == [3.0, 4.0]  # the two newest, in order


def test_snapshot_last_n_larger_than_buffer_returns_all() -> None:
    collector = MetricsCollector()
    for i in range(3):
        _record(collector, ts=float(i), throughput=float(i))

    assert len(collector.snapshot(99)) == 3


def test_snapshot_zero_returns_empty() -> None:
    collector = MetricsCollector()
    for i in range(3):
        _record(collector, ts=float(i), throughput=float(i))

    assert collector.snapshot(0) == []


def test_recent_throughput_averages_last_n() -> None:
    collector = MetricsCollector()
    # throughputs: 0,10,20,30,40 -> last 3 = 20,30,40 -> mean 30
    for i in range(5):
        _record(collector, ts=float(i), throughput=float(i * 10))

    assert collector.recent_throughput(3) == pytest.approx(30.0)


def test_recent_throughput_default_window_is_five() -> None:
    collector = MetricsCollector()
    # throughputs 0..9 -> last 5 = 5,6,7,8,9 -> mean 7.0
    for i in range(10):
        _record(collector, ts=float(i), throughput=float(i))

    assert collector.recent_throughput() == pytest.approx(7.0)


def test_recent_throughput_empty_returns_zero() -> None:
    assert MetricsCollector().recent_throughput() == pytest.approx(0.0)


def test_to_series_returns_parallel_lists_with_expected_keys() -> None:
    collector = MetricsCollector()
    collector.record_metrics(
        timestamp=1.0,
        batch_size=100,
        throughput=500.0,
        latency_ms=11.0,
        cpu_percent=40.0,
        memory_percent=50.0,
        memory_available_mb=2048.0,
    )
    collector.record_metrics(
        timestamp=2.0,
        batch_size=110,
        throughput=600.0,
        latency_ms=12.0,
        cpu_percent=41.0,
        memory_percent=51.0,
        memory_available_mb=2000.0,
    )

    series = collector.to_series()

    assert set(series.keys()) == {
        "timestamp",
        "batch_size",
        "throughput",
        "latency_ms",
        "cpu_percent",
        "memory_percent",
    }
    # Parallel lists, oldest -> newest.
    assert series["timestamp"] == [1.0, 2.0]
    assert series["batch_size"] == [100, 110]
    assert series["throughput"] == [500.0, 600.0]
    assert series["latency_ms"] == [11.0, 12.0]
    assert series["cpu_percent"] == [40.0, 41.0]
    assert series["memory_percent"] == [50.0, 51.0]
    # Every list has equal length (truly parallel).
    assert len({len(v) for v in series.values()}) == 1


def test_to_series_last_n_limits_points() -> None:
    collector = MetricsCollector()
    for i in range(5):
        _record(collector, ts=float(i), throughput=float(i))

    series = collector.to_series(2)

    assert series["timestamp"] == [3.0, 4.0]
    assert all(len(v) == 2 for v in series.values())


def test_to_series_empty_returns_empty_lists() -> None:
    series = MetricsCollector().to_series()
    assert set(series.keys()) == {
        "timestamp",
        "batch_size",
        "throughput",
        "latency_ms",
        "cpu_percent",
        "memory_percent",
    }
    assert all(v == [] for v in series.values())


def test_set_queue_depth_and_property() -> None:
    collector = MetricsCollector()
    assert collector.queue_depth == 0  # initial

    collector.set_queue_depth(42)
    assert collector.queue_depth == 42

    # Coerced to int.
    collector.set_queue_depth(7.9)
    assert collector.queue_depth == 7
    assert isinstance(collector.queue_depth, int)


def test_clear_empties_buffer() -> None:
    collector = MetricsCollector()
    for i in range(3):
        _record(collector, ts=float(i), throughput=float(i))
    assert len(collector) == 3

    collector.clear()

    assert len(collector) == 0
    assert collector.latest() is None
    assert collector.snapshot() == []


def test_maxlen_rollover_drops_oldest() -> None:
    """With maxlen=3, recording 5 keeps only the 3 newest; the oldest two drop."""
    collector = MetricsCollector(maxlen=3)
    for i in range(5):
        _record(collector, ts=float(i), throughput=float(i * 100))

    assert len(collector) == 3

    snaps = collector.snapshot()
    # ts 0.0 and 1.0 were evicted; 2.0, 3.0, 4.0 survive in order.
    assert [s.timestamp for s in snaps] == [2.0, 3.0, 4.0]
    assert [s.throughput for s in snaps] == [200.0, 300.0, 400.0]
    assert collector.latest().timestamp == 4.0


def test_default_maxlen_matches_settings_history_size() -> None:
    """Default buffer length tracks settings.metrics_history_size (200)."""
    from src.settings import get_settings

    collector = MetricsCollector()
    limit = get_settings().metrics_history_size
    for i in range(limit + 10):
        _record(collector, ts=float(i), throughput=float(i))

    assert len(collector) == limit
    # Oldest 10 dropped; newest is the last recorded.
    assert collector.latest().timestamp == float(limit + 9)
