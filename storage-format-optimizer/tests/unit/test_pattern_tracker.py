"""Unit tests for :mod:`src.pattern_tracker`.

Drives :class:`PatternTracker` with a controllable, mutable clock so timestamps
are deterministic. Covers write/read ratios, point-lookup vs. scan accounting,
column-touch counting (including the ``columns=None`` full-record case), tenant
isolation, and that the injected clock controls ``last_access``.
"""
from __future__ import annotations

from src.models import QueryClass
from src.pattern_tracker import PatternTracker


class _Clock:
    """Mutable, list-free controllable clock; set ``.now`` to steer the time."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def _read(
    tracker: PatternTracker,
    tenant: str,
    part: str,
    *,
    columns: list[str] | None = None,
    is_point_lookup: bool = False,
    query_class: QueryClass = QueryClass.ANALYTICAL,
) -> None:
    tracker.record_read(
        tenant,
        part,
        columns=columns,
        query_class=query_class,
        is_point_lookup=is_point_lookup,
    )


def test_write_and_read_counts_and_ratio() -> None:
    tracker = PatternTracker(clock=_Clock())
    for _ in range(3):
        tracker.record_write("t", "p")
    for _ in range(7):
        _read(tracker, "t", "p")

    stats = tracker.get_stats("t", "p")
    assert stats.writes == 3
    assert stats.reads == 7
    assert stats.write_ratio == 3 / 10


def test_point_lookup_vs_scan_ratio() -> None:
    tracker = PatternTracker(clock=_Clock())
    for _ in range(2):
        _read(tracker, "t", "p", is_point_lookup=True)
    for _ in range(3):
        _read(tracker, "t", "p", is_point_lookup=False)

    stats = tracker.get_stats("t", "p")
    assert stats.point_lookups == 2
    assert stats.scans == 3
    assert stats.point_lookup_ratio == 0.4


def test_column_touch_counting() -> None:
    tracker = PatternTracker(clock=_Clock())
    _read(tracker, "t", "p", columns=["a", "b"])
    _read(tracker, "t", "p", columns=["a"])

    stats = tracker.get_stats("t", "p")
    assert stats.column_counter["a"] == 2
    assert stats.column_counter["b"] == 1
    assert stats.distinct_columns == 2
    # (2 + 1) columns over 2 reads.
    assert stats.avg_columns_touched == 1.5


def test_none_columns_read_counts_but_adds_no_columns() -> None:
    tracker = PatternTracker(clock=_Clock())
    _read(tracker, "t", "p", columns=["a", "b"])
    before_distinct = tracker.get_stats("t", "p").distinct_columns

    _read(tracker, "t", "p", columns=None)
    stats = tracker.get_stats("t", "p")

    assert stats.reads == 2
    # A full-record (None) read widens neither the distinct set...
    assert stats.distinct_columns == before_distinct == 2
    # ...nor the columns-per-read total (still 2 from the first read only).
    assert stats.columns_per_read_total == 2


def test_tenant_isolation_and_listing() -> None:
    tracker = PatternTracker(clock=_Clock())
    tracker.record_write("t1", "p")
    _read(tracker, "t2", "p")

    s1 = tracker.get_stats("t1", "p")
    s2 = tracker.get_stats("t2", "p")
    assert (s1.writes, s1.reads) == (1, 0)
    assert (s2.writes, s2.reads) == (0, 1)

    tenants = tracker.all_tenants()
    assert "t1" in tenants
    assert "t2" in tenants


def test_injected_clock_controls_last_access() -> None:
    clock = _Clock()
    tracker = PatternTracker(clock=clock)
    clock.now = 123.0
    _read(tracker, "t", "p")

    assert tracker.get_stats("t", "p").last_access == 123.0
