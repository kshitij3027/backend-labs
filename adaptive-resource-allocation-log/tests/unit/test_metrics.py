"""Unit tests for :mod:`src.metrics`.

These tests use lightweight inline fakes for the workload model and worker pool so
they stay hermetic and do not depend on ``src.workers`` / ``src.load_model`` (written
concurrently by other agents). ``psutil`` is exercised against the real host but only
for type/range assertions, never for exact values.
"""

import math

from src.config import Settings
from src.metrics import SNAPSHOT_KEYS, MetricCollector, RollingHistory


# --------------------------------------------------------------------------- #
# Inline fakes (duck-typed collaborators)
# --------------------------------------------------------------------------- #
class FakeLoadModel:
    """Returns a fixed arrival rate regardless of ``now``."""

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.last_now = None

    def arrival_rate(self, now: float) -> float:
        self.last_now = now
        return self.rate


class FakeWorkerPool:
    """Returns a fixed worker count and a fixed ``stats()`` payload."""

    def __init__(self, workers: int, stats: dict) -> None:
        self._workers = workers
        self._stats = stats

    def current(self) -> int:
        return self._workers

    def stats(self) -> dict:
        return dict(self._stats)


class TinyConfig:
    """Minimal stand-in for Settings exposing only capacity_per_worker."""

    def __init__(self, capacity_per_worker: float) -> None:
        self.capacity_per_worker = capacity_per_worker


def _pool(workers=2, queue_depth=7, throughput=750.0, latency_ms=12.5, capacity=800.0):
    return FakeWorkerPool(
        workers,
        {
            "queue_depth": queue_depth,
            "throughput": throughput,
            "latency_ms": latency_ms,
            "capacity": capacity,
        },
    )


# --------------------------------------------------------------------------- #
# MetricCollector.sample()
# --------------------------------------------------------------------------- #
def test_sample_returns_all_canonical_keys_with_types():
    config = Settings(capacity_per_worker=400.0)
    collector = MetricCollector(config, FakeLoadModel(800.0), _pool(workers=2))

    snap = collector.sample(now=1000.0)

    # Every canonical key present, no extras.
    assert set(snap.keys()) == set(SNAPSHOT_KEYS)

    assert isinstance(snap["timestamp"], float)
    assert snap["timestamp"] == 1000.0

    assert isinstance(snap["cpu_percent"], float)

    assert isinstance(snap["cpu_per_core"], list)
    assert all(isinstance(c, float) for c in snap["cpu_per_core"])

    assert isinstance(snap["memory_percent"], float)
    assert 0.0 <= snap["memory_percent"] <= 100.0

    assert isinstance(snap["load_avg"], list)
    assert len(snap["load_avg"]) == 3
    assert all(isinstance(x, float) for x in snap["load_avg"])

    assert isinstance(snap["arrival_rate"], float)
    assert isinstance(snap["workers"], int)
    assert isinstance(snap["capacity_per_worker"], float)
    assert isinstance(snap["effective_utilization"], float)
    assert isinstance(snap["queue_depth"], int)
    assert isinstance(snap["throughput"], float)
    assert isinstance(snap["latency_ms"], float)


def test_effective_utilization_at_capacity_is_100():
    config = Settings(capacity_per_worker=400.0)
    collector = MetricCollector(config, FakeLoadModel(800.0), _pool(workers=2))

    snap = collector.sample(now=1.0)

    # 800 / (2 * 400) * 100 == 100.0
    assert snap["effective_utilization"] == 100.0


def test_effective_utilization_overload_not_capped():
    config = Settings(capacity_per_worker=400.0)
    collector = MetricCollector(config, FakeLoadModel(1600.0), _pool(workers=2))

    snap = collector.sample(now=1.0)

    # 1600 / (2 * 400) * 100 == 200.0 — overload is reported, never clamped to 100.
    assert snap["effective_utilization"] == 200.0


def test_effective_utilization_zero_workers_guarded():
    config = Settings(capacity_per_worker=400.0)
    collector = MetricCollector(config, FakeLoadModel(500.0), _pool(workers=0))

    # Must not raise ZeroDivisionError; denominator is floored at 1.0. This is an
    # unreachable guard branch in practice (min_workers is 2), so assert behaviour
    # robustly rather than pinning the exact magnitude: it returns a large, finite
    # overload value (500 / max(1.0, 0*400) * 100 == 50000.0).
    snap = collector.sample(now=1.0)
    assert snap["workers"] == 0
    assert math.isfinite(snap["effective_utilization"])
    assert snap["effective_utilization"] >= 100.0


def test_stats_fields_taken_from_pool():
    config = Settings(capacity_per_worker=400.0)
    pool = _pool(workers=3, queue_depth=42, throughput=1234.5, latency_ms=9.0)
    collector = MetricCollector(config, FakeLoadModel(100.0), pool)

    snap = collector.sample(now=1.0)

    assert snap["queue_depth"] == 42
    assert snap["throughput"] == 1234.5
    assert snap["latency_ms"] == 9.0
    assert snap["workers"] == 3


def test_missing_stats_keys_default_to_zero():
    config = Settings(capacity_per_worker=400.0)
    # stats() returns an empty dict — every derived field must fall back gracefully.
    pool = FakeWorkerPool(2, {})
    collector = MetricCollector(config, FakeLoadModel(0.0), pool)

    snap = collector.sample(now=1.0)

    assert snap["queue_depth"] == 0
    assert snap["throughput"] == 0.0
    assert snap["latency_ms"] == 0.0


def test_queue_depth_coerced_to_int():
    config = Settings(capacity_per_worker=400.0)
    pool = FakeWorkerPool(
        2, {"queue_depth": 5.9, "throughput": 1.0, "latency_ms": 1.0, "capacity": 1.0}
    )
    collector = MetricCollector(config, FakeLoadModel(0.0), pool)

    snap = collector.sample(now=1.0)
    assert isinstance(snap["queue_depth"], int)
    assert snap["queue_depth"] == 5  # int() truncates


def test_sample_uses_supplied_now_for_arrival_rate():
    config = Settings(capacity_per_worker=400.0)
    model = FakeLoadModel(123.0)
    collector = MetricCollector(config, model, _pool())

    collector.sample(now=4242.0)
    assert model.last_now == 4242.0


def test_sample_defaults_now_to_time(monkeypatch):
    import src.metrics as metrics_mod

    monkeypatch.setattr(metrics_mod.time, "time", lambda: 5555.0)
    config = Settings(capacity_per_worker=400.0)
    collector = MetricCollector(config, FakeLoadModel(10.0), _pool())

    snap = collector.sample()
    assert snap["timestamp"] == 5555.0


def test_works_with_tiny_config():
    # The collector only needs capacity_per_worker; a tiny config suffices.
    collector = MetricCollector(TinyConfig(200.0), FakeLoadModel(400.0), _pool(workers=1))
    snap = collector.sample(now=1.0)
    # 400 / (1 * 200) * 100 == 200.0
    assert snap["effective_utilization"] == 200.0
    assert snap["capacity_per_worker"] == 200.0


# --------------------------------------------------------------------------- #
# RollingHistory
# --------------------------------------------------------------------------- #
def _snap(ts, **fields):
    base = {"timestamp": float(ts)}
    base.update(fields)
    return base


def test_history_len_and_latest():
    hist = RollingHistory(window_minutes=15, retention_hours=24)
    assert len(hist) == 0
    assert hist.latest() is None

    hist.add(_snap(1.0, cpu_percent=10.0))
    hist.add(_snap(2.0, cpu_percent=20.0))

    assert len(hist) == 2
    assert hist.latest()["timestamp"] == 2.0
    assert hist.latest()["cpu_percent"] == 20.0


def test_history_window_filters_old_entries():
    hist = RollingHistory(window_minutes=15, retention_hours=24)
    # Timestamps span ~2 minutes; newest is at t=120.
    hist.add(_snap(0.0, cpu_percent=1.0))     # 120s before newest -> outside 1 min
    hist.add(_snap(70.0, cpu_percent=2.0))    # 50s before newest  -> inside 1 min
    hist.add(_snap(120.0, cpu_percent=3.0))   # newest

    recent = hist.window(minutes=1)
    timestamps = [s["timestamp"] for s in recent]

    assert 0.0 not in timestamps        # evicted from the 1-minute window
    assert timestamps == [70.0, 120.0]  # oldest -> newest, within window


def test_history_window_defaults_to_window_minutes():
    hist = RollingHistory(window_minutes=2, retention_hours=24)
    hist.add(_snap(0.0))      # 5 min before newest -> outside default 2-min window
    hist.add(_snap(290.0))    # 10s before newest   -> inside
    hist.add(_snap(300.0))    # newest

    recent = hist.window()  # no arg -> uses window_minutes == 2
    assert [s["timestamp"] for s in recent] == [290.0, 300.0]


def test_history_series_last_n_oldest_to_newest():
    hist = RollingHistory(window_minutes=15, retention_hours=24)
    for i, cpu in enumerate([10.0, 20.0, 30.0, 40.0, 50.0]):
        hist.add(_snap(float(i), cpu_percent=cpu))

    series = hist.series("cpu_percent", points=3)
    # Last 3 values, oldest -> newest.
    assert series == [30.0, 40.0, 50.0]
    assert all(isinstance(v, float) for v in series)


def test_history_series_skips_missing_field():
    hist = RollingHistory(window_minutes=15, retention_hours=24)
    hist.add(_snap(0.0, cpu_percent=10.0))
    hist.add(_snap(1.0))                       # no cpu_percent -> skipped
    hist.add(_snap(2.0, cpu_percent=30.0))

    series = hist.series("cpu_percent", points=60)
    assert series == [10.0, 30.0]


def test_history_series_returns_floats_for_int_values():
    hist = RollingHistory()
    hist.add(_snap(0.0, queue_depth=5))
    hist.add(_snap(1.0, queue_depth=9))

    series = hist.series("queue_depth", points=60)
    assert series == [5.0, 9.0]
    assert all(isinstance(v, float) for v in series)


def test_history_retention_evicts_old_entries():
    # 1-hour retention: anything older than 1h before the newest is dropped.
    hist = RollingHistory(window_minutes=15, retention_hours=1)
    hist.add(_snap(0.0, cpu_percent=1.0))         # t=0
    hist.add(_snap(1800.0, cpu_percent=2.0))      # +30 min
    assert len(hist) == 2

    # New sample at t = 2 hours. Cutoff = 7200 - 3600 = 3600, so every entry with
    # timestamp < 3600 is evicted -> both t=0 AND t=1800 are dropped, leaving [7200].
    hist.add(_snap(7200.0, cpu_percent=3.0))
    timestamps = [s["timestamp"] for s in hist.window(minutes=10_000)]
    assert 0.0 not in timestamps
    assert timestamps == [7200.0]
    assert len(hist) == 1


def test_history_empty_window_and_series():
    hist = RollingHistory()
    assert hist.window() == []
    assert hist.window(minutes=5) == []
    assert hist.series("cpu_percent") == []
    assert hist.series("cpu_percent", points=0) == []
