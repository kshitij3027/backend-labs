from __future__ import annotations

from datetime import datetime

import pytest

from src.parser import parse_sql
from src.partition.data_generator import generate_logs
from src.partition.executor import LocalExecutor
from src.partition.storage import LogStorage
from src.planner.planner import serialize_ast
from src.shared.models import TimeRange


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def time_range() -> TimeRange:
    return TimeRange(
        start=datetime(2026, 4, 1, 0, 0, 0),
        end=datetime(2026, 4, 7, 23, 59, 59),
    )


@pytest.fixture
def records(time_range: TimeRange) -> list[dict]:
    return generate_logs("partition-1", time_range, 200)


@pytest.fixture
def storage(records: list[dict]) -> LogStorage:
    return LogStorage(
        records=records, indexed_fields=["level", "service", "timestamp"]
    )


@pytest.fixture
def executor(storage: LogStorage) -> LocalExecutor:
    return LocalExecutor(storage=storage)


def _where_filter(sql_tail: str) -> dict:
    """Parse ``SELECT * FROM logs WHERE <sql_tail>`` and return the
    serialized WHERE AST — the exact payload the planner pushes down.
    """

    node = parse_sql(f"SELECT * FROM logs WHERE {sql_tail}")
    blob = serialize_ast(node.where)
    assert blob is not None
    return blob


# ---------------------------------------------------------------------------
# basic binop coverage
# ---------------------------------------------------------------------------


def test_filter_none_returns_all(
    executor: LocalExecutor, records: list[dict]
) -> None:
    assert executor.filter(None) == records


def test_eq(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("level = 'ERROR'")
    got = executor.filter(filt)
    expected = [r for r in records if r["level"] == "ERROR"]
    assert got == expected
    assert all(r["level"] == "ERROR" for r in got)


def test_neq(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("level != 'ERROR'")
    got = executor.filter(filt)
    assert all(r["level"] != "ERROR" for r in got)
    assert len(got) == sum(1 for r in records if r["level"] != "ERROR")


def test_gt(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("status_code > 400")
    got = executor.filter(filt)
    assert all(int(r["status_code"]) > 400 for r in got)
    expected = [r for r in records if int(r["status_code"]) > 400]
    assert len(got) == len(expected)


def test_lt(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("status_code < 400")
    got = executor.filter(filt)
    assert all(int(r["status_code"]) < 400 for r in got)


def test_ge(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("duration_ms >= 50")
    got = executor.filter(filt)
    assert all(r["duration_ms"] >= 50 for r in got)


def test_le(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("duration_ms <= 50")
    got = executor.filter(filt)
    assert all(r["duration_ms"] <= 50 for r in got)


# ---------------------------------------------------------------------------
# IN / BETWEEN / CONTAINS / NOT
# ---------------------------------------------------------------------------


def test_in(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("level IN ('ERROR', 'WARN')")
    got = executor.filter(filt)
    assert all(r["level"] in {"ERROR", "WARN"} for r in got)
    assert len(got) == sum(
        1 for r in records if r["level"] in {"ERROR", "WARN"}
    )


def test_between(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("duration_ms BETWEEN 10 AND 80")
    got = executor.filter(filt)
    assert all(10 <= r["duration_ms"] <= 80 for r in got)


def test_contains(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("message CONTAINS 'timeout'")
    got = executor.filter(filt)
    # The generator seeds enough timeout messages that there must be hits.
    assert got, "expected at least one 'timeout' row"
    assert all("timeout" in r["message"].lower() for r in got)


def test_contains_is_case_insensitive(
    executor: LocalExecutor, records: list[dict]
) -> None:
    filt = _where_filter("message CONTAINS 'TIMEOUT'")
    got = executor.filter(filt)
    assert got
    assert all("timeout" in r["message"].lower() for r in got)


def test_not(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("NOT (level = 'ERROR')")
    got = executor.filter(filt)
    assert all(r["level"] != "ERROR" for r in got)


# ---------------------------------------------------------------------------
# boolean combinators
# ---------------------------------------------------------------------------


def test_and(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("level = 'ERROR' AND service = 'api'")
    got = executor.filter(filt)
    assert all(r["level"] == "ERROR" and r["service"] == "api" for r in got)
    expected = [
        r for r in records if r["level"] == "ERROR" and r["service"] == "api"
    ]
    assert len(got) == len(expected)


def test_or(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter("level = 'ERROR' OR service = 'api'")
    got = executor.filter(filt)
    assert all(r["level"] == "ERROR" or r["service"] == "api" for r in got)
    expected = [
        r for r in records if r["level"] == "ERROR" or r["service"] == "api"
    ]
    assert len(got) == len(expected)


def test_nested_and_or(executor: LocalExecutor, records: list[dict]) -> None:
    filt = _where_filter(
        "(level = 'ERROR' AND service = 'api') "
        "OR (level = 'WARN' AND duration_ms > 100)"
    )
    got = executor.filter(filt)
    expected = [
        r
        for r in records
        if (r["level"] == "ERROR" and r["service"] == "api")
        or (r["level"] == "WARN" and r["duration_ms"] > 100)
    ]
    assert len(got) == len(expected)
    for row in got:
        ok = (row["level"] == "ERROR" and row["service"] == "api") or (
            row["level"] == "WARN" and row["duration_ms"] > 100
        )
        assert ok


def test_and_with_timestamp_range_uses_index(
    executor: LocalExecutor, records: list[dict]
) -> None:
    # Mid-dataset cutoff so both branches of the AND are real filters.
    pivot = records[len(records) // 2]["timestamp"]
    filt = _where_filter(
        f"level = 'ERROR' AND timestamp >= '{pivot}'"
    )
    got = executor.filter(filt)
    expected = [
        r for r in records if r["level"] == "ERROR" and r["timestamp"] >= pivot
    ]
    assert sorted(r["timestamp"] for r in got) == sorted(
        r["timestamp"] for r in expected
    )


# ---------------------------------------------------------------------------
# partial aggregates
# ---------------------------------------------------------------------------


def test_partial_aggregate_count(
    executor: LocalExecutor, records: list[dict]
) -> None:
    agg = {"functions": [["COUNT", "*"]], "group_by": []}
    result = executor.partial_aggregate(records, agg)

    assert result["groups"] is None
    assert result["record_count"] == len(records)
    assert result["aggregates"] == {"COUNT(*)": len(records)}


def test_partial_aggregate_sum_min_max(
    executor: LocalExecutor, records: list[dict]
) -> None:
    agg = {
        "functions": [
            ["SUM", "duration_ms"],
            ["MIN", "duration_ms"],
            ["MAX", "duration_ms"],
        ],
        "group_by": [],
    }
    result = executor.partial_aggregate(records, agg)

    durations = [r["duration_ms"] for r in records]
    assert result["aggregates"]["SUM(duration_ms)"] == sum(durations)
    assert result["aggregates"]["MIN(duration_ms)"] == min(durations)
    assert result["aggregates"]["MAX(duration_ms)"] == max(durations)
    assert result["sums"]["duration_ms"] == sum(durations)
    assert result["mins"]["duration_ms"] == min(durations)
    assert result["maxs"]["duration_ms"] == max(durations)


def test_partial_aggregate_empty_rows(executor: LocalExecutor) -> None:
    agg = {
        "functions": [["COUNT", "*"], ["SUM", "duration_ms"]],
        "group_by": [],
    }
    result = executor.partial_aggregate([], agg)
    assert result["record_count"] == 0
    assert result["aggregates"]["COUNT(*)"] == 0
    assert result["aggregates"]["SUM(duration_ms)"] == 0


def test_partial_aggregate_none(executor: LocalExecutor) -> None:
    assert executor.partial_aggregate([{}, {}], None) == {}


def test_partial_aggregate_group_by_service(
    executor: LocalExecutor, records: list[dict]
) -> None:
    agg = {
        "functions": [["COUNT", "*"], ["SUM", "duration_ms"]],
        "group_by": ["service"],
    }
    result = executor.partial_aggregate(records, agg)

    assert result["aggregates"] is None
    assert result["group_by"] == ["service"]
    assert result["record_count"] == len(records)

    groups = result["groups"]
    assert groups is not None

    # Buckets expected to cover every distinct service seen in the rows.
    distinct = {r["service"] for r in records}
    assert set(groups.keys()) == distinct  # single-field group_by uses raw value

    for service in distinct:
        bucket = groups[service]
        expected_count = sum(1 for r in records if r["service"] == service)
        expected_sum = sum(
            r["duration_ms"] for r in records if r["service"] == service
        )
        assert bucket["count"] == expected_count
        assert bucket["sums"]["duration_ms"] == expected_sum
        assert bucket["group_values"] == {"service": service}


def test_partial_aggregate_group_by_two_keys_uses_separator(
    executor: LocalExecutor, records: list[dict]
) -> None:
    agg = {
        "functions": [["COUNT", "*"]],
        "group_by": ["service", "level"],
    }
    result = executor.partial_aggregate(records, agg)
    groups = result["groups"]
    assert groups is not None

    expected = {}
    for row in records:
        key = f"{row['service']}\u0001{row['level']}"
        expected[key] = expected.get(key, 0) + 1

    assert set(groups.keys()) == set(expected.keys())
    for key, bucket in groups.items():
        assert bucket["count"] == expected[key]


# ---------------------------------------------------------------------------
# small correctness guards
# ---------------------------------------------------------------------------


def test_filter_with_explicit_row_list(
    executor: LocalExecutor, records: list[dict]
) -> None:
    # Restrict the search space via the ``rows=`` shortcut.
    subset = records[:10]
    filt = _where_filter("level = 'ERROR'")
    got = executor.filter(filt, rows=subset)
    assert all(r in subset for r in got)
    assert all(r["level"] == "ERROR" for r in got)


def test_numeric_string_coercion(executor: LocalExecutor) -> None:
    """`status_code = 500` should match whether status_code is an int or a
    numeric string in the underlying row."""

    storage = LogStorage(
        records=[
            {"status_code": 500, "level": "ERROR", "service": "api"},
            {"status_code": "500", "level": "ERROR", "service": "api"},
            {"status_code": 200, "level": "INFO", "service": "api"},
        ],
        indexed_fields=["level", "service"],
    )
    exec2 = LocalExecutor(storage=storage)

    filt = _where_filter("status_code = 500")
    got = exec2.filter(filt)
    assert len(got) == 2
