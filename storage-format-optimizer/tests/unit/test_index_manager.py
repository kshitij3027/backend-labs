"""Unit tests for :mod:`src.index_manager` (C13).

Covers the :class:`~src.index_manager.IndexManager` build/drop policy and the
:class:`~src.index_manager.PartitionIndex` min/max builder. The manager is a
pure, deterministic function of the recorded history (no clock, no I/O), so the
tests just feed it observations and assert the gate outcomes.

Config used throughout:
    * ``min_filter_hits     == 5``
    * ``min_selectivity     == 0.2``
    * ``drop_benefit_window == 200``
    * ``drop_min_benefit    == 0.01``
"""
from __future__ import annotations

from src.index_manager import IndexManager, PartitionIndex


def _manager() -> IndexManager:
    """Return an IndexManager with the canonical thresholds."""
    return IndexManager(
        min_filter_hits=5,
        min_selectivity=0.2,
        drop_benefit_window=200,
        drop_min_benefit=0.01,
    )


def test_should_build_requires_frequency_and_selectivity() -> None:
    mgr = _manager()

    # Below the hit threshold -> not built (4 selective hits).
    for _ in range(4):
        mgr.note_filter("t", "p", "col", selectivity=0.05)
    assert mgr.should_build("t", "p", "col") is False

    # The 5th selective hit crosses min_filter_hits with mean selectivity low.
    mgr.note_filter("t", "p", "col", selectivity=0.05)
    assert mgr.should_build("t", "p", "col") is True


def test_should_not_build_when_not_selective() -> None:
    mgr = _manager()

    # Frequent (5 hits) but not selective (mean 0.9 > 0.2) -> not worth indexing.
    for _ in range(5):
        mgr.note_filter("t", "p", "col", selectivity=0.9)
    assert mgr.should_build("t", "p", "col") is False


def test_candidate_columns_returns_buildable_only() -> None:
    mgr = _manager()

    # "good" is frequent + selective; "bad" is frequent but not selective.
    for _ in range(5):
        mgr.note_filter("t", "p", "good", selectivity=0.05)
        mgr.note_filter("t", "p", "bad", selectivity=0.9)

    candidates = mgr.candidate_columns("t", "p")
    assert candidates == ["good"]


def test_should_drop_keeps_high_benefit_drops_low() -> None:
    mgr = _manager()

    # High skip-fraction (0.9) repeated -> the index is pulling its weight.
    for _ in range(10):
        mgr.record_benefit("t", "p", "keep", rows_skipped=90, rows_total=100)
    assert mgr.should_drop("t", "p", "keep") is False

    # Near-zero skip-fraction (0.0) repeated -> no longer earning its keep.
    for _ in range(10):
        mgr.record_benefit("t", "p", "drop", rows_skipped=0, rows_total=100)
    assert mgr.should_drop("t", "p", "drop") is True

    # prune() returns exactly the columns that should be dropped.
    assert mgr.prune("t", "p", ["drop"]) == ["drop"]
    assert mgr.prune("t", "p", ["keep"]) == []


def test_should_drop_false_without_history() -> None:
    mgr = _manager()
    # No benefit recorded yet -> the index has not had a chance to prove itself.
    assert mgr.should_drop("t", "p", "col") is False


def test_partition_index_from_rows() -> None:
    rows = [{"a": 3, "b": "x"}, {"a": 7, "b": "y"}, {"a": 1}]
    idx = PartitionIndex.from_rows(rows, ["a", "b", "missing"])

    # "a" and "b" carry usable bounds; "missing" never appears -> skipped.
    assert "a" in idx.columns
    assert "b" in idx.columns
    assert "missing" not in idx.columns

    assert idx.stats["a"] == {"min": 1, "max": 7}
    assert idx.stats["b"] == {"min": "x", "max": "y"}

    d = idx.to_dict()
    assert set(d) == {"columns", "stats"}
    assert d["columns"] == list(idx.columns)
    assert d["stats"]["a"] == {"min": 1, "max": 7}
    assert d["stats"]["b"] == {"min": "x", "max": "y"}
