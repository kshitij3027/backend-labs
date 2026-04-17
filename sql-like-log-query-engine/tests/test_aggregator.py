"""Tests for the coordinator's result-merge aggregator."""

from __future__ import annotations

import pytest

from src.coordinator.aggregator import merge
from src.parser import parse_sql
from src.shared.models import PartitionExecuteResponse


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _ungrouped_partial(
    *,
    count: int,
    sums: dict[str, float] | None = None,
    mins: dict[str, float] | None = None,
    maxs: dict[str, float] | None = None,
    functions: list[list[str]] | None = None,
    records_scanned: int | None = None,
) -> PartitionExecuteResponse:
    return PartitionExecuteResponse(
        rows=[],
        partial_aggregate={
            "groups": None,
            "aggregates": None,
            "record_count": count,
            "count": count,
            "sums": sums or {},
            "mins": mins or {},
            "maxs": maxs or {},
            "functions": functions or [["COUNT", "*"]],
            "group_by": [],
        },
        records_scanned=records_scanned if records_scanned is not None else count,
    )


def _grouped_partial(
    *,
    groups: dict[str, dict],
    functions: list[list[str]],
    group_by: list[str],
    record_count: int,
    records_scanned: int | None = None,
) -> PartitionExecuteResponse:
    return PartitionExecuteResponse(
        rows=[],
        partial_aggregate={
            "groups": groups,
            "aggregates": None,
            "record_count": record_count,
            "functions": functions,
            "group_by": group_by,
        },
        records_scanned=records_scanned if records_scanned is not None else record_count,
    )


# ---------------------------------------------------------------------------
# COUNT(*) across 3 partitions
# ---------------------------------------------------------------------------


def test_count_star_merges_across_partitions() -> None:
    ast_root = parse_sql("SELECT COUNT(*) FROM logs")

    partials = [
        _ungrouped_partial(count=5),
        _ungrouped_partial(count=8),
        _ungrouped_partial(count=12),
    ]

    rows = merge(partials, ast_root)
    assert rows == [{"COUNT(*)": 25}]


def test_count_star_respects_alias() -> None:
    ast_root = parse_sql("SELECT COUNT(*) AS total FROM logs")
    partials = [_ungrouped_partial(count=3), _ungrouped_partial(count=4)]
    assert merge(partials, ast_root) == [{"total": 7}]


# ---------------------------------------------------------------------------
# SUM / MIN / MAX / AVG
# ---------------------------------------------------------------------------


def test_sum_min_max_avg_on_duration_ms() -> None:
    ast_root = parse_sql(
        "SELECT SUM(duration_ms) AS s, MIN(duration_ms) AS mn, "
        "MAX(duration_ms) AS mx, AVG(duration_ms) AS av FROM logs"
    )

    partials = [
        _ungrouped_partial(
            count=2,
            sums={"duration_ms": 10.0},
            mins={"duration_ms": 3.0},
            maxs={"duration_ms": 7.0},
            functions=[
                ["SUM", "duration_ms"],
                ["MIN", "duration_ms"],
                ["MAX", "duration_ms"],
                ["AVG", "duration_ms"],
            ],
        ),
        _ungrouped_partial(
            count=3,
            sums={"duration_ms": 90.0},
            mins={"duration_ms": 20.0},
            maxs={"duration_ms": 50.0},
            functions=[
                ["SUM", "duration_ms"],
                ["MIN", "duration_ms"],
                ["MAX", "duration_ms"],
                ["AVG", "duration_ms"],
            ],
        ),
    ]

    rows = merge(partials, ast_root)
    assert len(rows) == 1
    row = rows[0]
    assert row["s"] == 100.0
    assert row["mn"] == 3.0
    assert row["mx"] == 50.0
    assert row["av"] == pytest.approx(100.0 / 5)  # 20.0


# ---------------------------------------------------------------------------
# GROUP BY
# ---------------------------------------------------------------------------


def test_group_by_service_unions_across_partitions() -> None:
    ast_root = parse_sql(
        "SELECT service, COUNT(*) AS n FROM logs GROUP BY service"
    )

    partials = [
        _grouped_partial(
            groups={
                "api": {
                    "count": 10,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "api"},
                },
                "auth": {
                    "count": 5,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "auth"},
                },
            },
            functions=[["COUNT", "*"]],
            group_by=["service"],
            record_count=15,
        ),
        _grouped_partial(
            groups={
                "api": {
                    "count": 20,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "api"},
                },
                "db": {
                    "count": 7,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "db"},
                },
            },
            functions=[["COUNT", "*"]],
            group_by=["service"],
            record_count=27,
        ),
    ]

    rows = merge(partials, ast_root)
    by_service = {row["service"]: row["n"] for row in rows}
    assert by_service == {"api": 30, "auth": 5, "db": 7}


def test_group_by_sums_and_avg_merge_correctly() -> None:
    ast_root = parse_sql(
        "SELECT service, AVG(duration_ms) AS avg_ms FROM logs GROUP BY service"
    )

    partials = [
        _grouped_partial(
            groups={
                "api": {
                    "count": 4,
                    "sums": {"duration_ms": 400.0},
                    "mins": {"duration_ms": 50.0},
                    "maxs": {"duration_ms": 200.0},
                    "group_values": {"service": "api"},
                }
            },
            functions=[["AVG", "duration_ms"]],
            group_by=["service"],
            record_count=4,
        ),
        _grouped_partial(
            groups={
                "api": {
                    "count": 6,
                    "sums": {"duration_ms": 600.0},
                    "mins": {"duration_ms": 10.0},
                    "maxs": {"duration_ms": 300.0},
                    "group_values": {"service": "api"},
                }
            },
            functions=[["AVG", "duration_ms"]],
            group_by=["service"],
            record_count=6,
        ),
    ]

    rows = merge(partials, ast_root)
    assert len(rows) == 1
    assert rows[0]["service"] == "api"
    assert rows[0]["avg_ms"] == pytest.approx(1000.0 / 10)  # 100.0


# ---------------------------------------------------------------------------
# HAVING
# ---------------------------------------------------------------------------


def test_having_filters_buckets_below_threshold() -> None:
    ast_root = parse_sql(
        "SELECT service, COUNT(*) AS n FROM logs GROUP BY service HAVING n > 10"
    )

    partials = [
        _grouped_partial(
            groups={
                "api": {
                    "count": 15,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "api"},
                },
                "auth": {
                    "count": 3,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "auth"},
                },
                "db": {
                    "count": 8,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "db"},
                },
            },
            functions=[["COUNT", "*"]],
            group_by=["service"],
            record_count=26,
        )
    ]

    rows = merge(partials, ast_root)
    assert rows == [{"service": "api", "n": 15}]


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT
# ---------------------------------------------------------------------------


def test_order_by_desc_and_limit_truncates() -> None:
    ast_root = parse_sql(
        "SELECT service, COUNT(*) AS n FROM logs "
        "GROUP BY service ORDER BY n DESC LIMIT 2"
    )

    partials = [
        _grouped_partial(
            groups={
                "api": {
                    "count": 5,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "api"},
                },
                "auth": {
                    "count": 12,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "auth"},
                },
                "db": {
                    "count": 3,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "db"},
                },
                "cache": {
                    "count": 20,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": {"service": "cache"},
                },
            },
            functions=[["COUNT", "*"]],
            group_by=["service"],
            record_count=40,
        )
    ]

    rows = merge(partials, ast_root)
    assert [row["service"] for row in rows] == ["cache", "auth"]
    assert [row["n"] for row in rows] == [20, 12]


# ---------------------------------------------------------------------------
# plain SELECT (no aggregation) — row concatenation
# ---------------------------------------------------------------------------


def test_plain_select_concatenates_rows() -> None:
    ast_root = parse_sql("SELECT * FROM logs LIMIT 5")

    partials = [
        PartitionExecuteResponse(
            rows=[
                {"level": "INFO", "service": "api"},
                {"level": "ERROR", "service": "api"},
            ],
            records_scanned=2,
        ),
        PartitionExecuteResponse(
            rows=[
                {"level": "WARN", "service": "auth"},
                {"level": "DEBUG", "service": "db"},
                {"level": "INFO", "service": "db"},
            ],
            records_scanned=3,
        ),
    ]

    rows = merge(partials, ast_root)
    assert len(rows) == 5
    assert {row["service"] for row in rows} == {"api", "auth", "db"}


def test_plain_select_columns_project_and_alias() -> None:
    ast_root = parse_sql("SELECT level AS lvl, service FROM logs LIMIT 10")
    partials = [
        PartitionExecuteResponse(
            rows=[
                {"level": "INFO", "service": "api", "message": "x", "duration_ms": 1},
                {"level": "ERROR", "service": "db", "message": "y", "duration_ms": 2},
            ],
            records_scanned=2,
        )
    ]
    rows = merge(partials, ast_root)
    assert rows == [
        {"lvl": "INFO", "service": "api"},
        {"lvl": "ERROR", "service": "db"},
    ]


def test_empty_partials_returns_empty() -> None:
    ast_root = parse_sql("SELECT * FROM logs")
    assert merge([], ast_root) == []


def test_empty_partials_with_aggregation_returns_zero_row() -> None:
    ast_root = parse_sql("SELECT COUNT(*) AS n FROM logs")
    # One partition reports zero rows.
    partials = [_ungrouped_partial(count=0)]
    rows = merge(partials, ast_root)
    assert rows == [{"n": 0}]
