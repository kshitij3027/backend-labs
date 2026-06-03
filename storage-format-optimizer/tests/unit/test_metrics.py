"""Unit tests for :class:`src.metrics.Metrics`.

Covers the analytical row-vs-columnar speedup, nearest-rank percentiles in the
snapshot, the windowed ingest rate under an injected clock, the compression
ratio, migration outcome accounting, the exact top-level snapshot keys, and the
history cap on every time-series deque.
"""
from __future__ import annotations

from src.metrics import Metrics


class _Clock:
    """Mutable controllable clock; set ``.now`` to steer the time."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def test_analytical_speedup_vs_row() -> None:
    m = Metrics()
    for _ in range(5):
        m.record_query("columnar", 10, query_class="analytical")
        m.record_query("row", 40, query_class="analytical")
    # row p50 (40) / columnar p50 (10) == 4.0
    assert m.analytical_speedup_vs_row() == 4.0


def test_percentiles_nearest_rank() -> None:
    m = Metrics()
    for ms in [10, 20, 30, 40, 50]:
        m.record_query("row", ms)
    row_perf = m.snapshot()["performance"]["by_format"]["row"]
    assert row_perf["p50"] == 30
    assert row_perf["p90"] == 50


def test_ingest_eps_window() -> None:
    clock = _Clock(start=1000.0)
    m = Metrics(clock=clock, window_seconds=60.0)

    # Inside the window: recorded at t, observed at t+10.
    m.record_ingest(100, ts=1000.0)
    clock.now = 1010.0
    assert m.ingest_eps() > 0

    # Outside the window: advance well past 60s so the event no longer counts.
    clock.now = 1000.0 + 100.0
    assert m.ingest_eps() == 0.0


def test_compression_ratio_and_storage_keys() -> None:
    m = Metrics()
    m.set_storage({"columnar": 400}, total_bytes=400, uncompressed_estimate_bytes=1000)
    assert m.compression_ratio() == 2.5

    by_format = m.snapshot()["storage"]["by_format"]
    assert set(by_format.keys()) == {"row", "columnar", "hybrid"}


def test_migration_outcomes() -> None:
    m = Metrics()
    m.record_migration(
        tenant="t",
        partition="p",
        from_fmt="row",
        to_fmt="columnar",
        ok=True,
        reason="seal",
    )
    m.record_migration(
        tenant="t",
        partition="p",
        from_fmt="row",
        to_fmt="columnar",
        ok=False,
        reason="error",
    )
    migrations = m.snapshot()["migrations"]
    assert migrations["completed"] == 1
    assert migrations["failed"] == 1
    assert len(migrations["recent"]) == 2


def test_snapshot_top_level_keys() -> None:
    m = Metrics()
    assert set(m.snapshot().keys()) == {
        "storage",
        "formats",
        "performance",
        "migrations",
        "ingest",
    }


def test_series_capped_at_history_points() -> None:
    m = Metrics(history_points=3)
    for _ in range(5):
        m.append_series_point()
    series = m.series()
    for values in series.values():
        assert len(values) == 3
