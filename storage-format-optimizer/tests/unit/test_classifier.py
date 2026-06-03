"""Unit tests for :func:`src.classifier.classify_query`.

Exercises the strict, first-match-wins rule ordering with the default
thresholds ``analytical_max=3`` / ``full_record_min=10``: column-count
boundaries (None / empty / narrow / mid / wide) and the aggregation override
that classifies as ANALYTICAL regardless of projection width.
"""
from __future__ import annotations

from src.classifier import classify_query
from src.models import Aggregation, QueryClass

ANALYTICAL_MAX = 3
FULL_RECORD_MIN = 10


def _classify(columns: list[str] | None, aggregations: list) -> QueryClass:
    return classify_query(
        columns,
        aggregations,
        analytical_max=ANALYTICAL_MAX,
        full_record_min=FULL_RECORD_MIN,
    )


def test_none_columns_is_full_record() -> None:
    assert _classify(None, []) == QueryClass.FULL_RECORD


def test_empty_columns_is_analytical() -> None:
    assert _classify([], []) == QueryClass.ANALYTICAL


def test_three_columns_is_analytical_boundary() -> None:
    cols = [f"c{i}" for i in range(3)]
    assert _classify(cols, []) == QueryClass.ANALYTICAL


def test_four_columns_is_mixed() -> None:
    cols = [f"c{i}" for i in range(4)]
    assert _classify(cols, []) == QueryClass.MIXED


def test_ten_columns_is_mixed_boundary() -> None:
    cols = [f"c{i}" for i in range(10)]
    assert _classify(cols, []) == QueryClass.MIXED


def test_eleven_columns_is_full_record() -> None:
    cols = [f"c{i}" for i in range(11)]
    assert _classify(cols, []) == QueryClass.FULL_RECORD


def test_aggregation_dict_wins_over_wide_projection() -> None:
    cols = [f"c{i}" for i in range(20)]
    assert _classify(cols, [{"op": "count"}]) == QueryClass.ANALYTICAL


def test_aggregation_model_wins_over_wide_projection() -> None:
    cols = [f"c{i}" for i in range(20)]
    assert _classify(cols, [Aggregation(op="count")]) == QueryClass.ANALYTICAL
