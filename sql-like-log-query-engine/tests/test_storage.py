from __future__ import annotations

from datetime import datetime

import pytest

from src.partition.data_generator import generate_logs
from src.partition.storage import LogStorage
from src.shared.models import TimeRange


@pytest.fixture
def time_range() -> TimeRange:
    return TimeRange(
        start=datetime(2026, 4, 1, 0, 0, 0),
        end=datetime(2026, 4, 7, 23, 59, 59),
    )


@pytest.fixture
def records(time_range: TimeRange) -> list[dict]:
    # Deterministic seed derived from the partition id → same dataset every run.
    return generate_logs("partition-1", time_range, 500)


@pytest.fixture
def storage(records: list[dict]) -> LogStorage:
    return LogStorage(
        records=records, indexed_fields=["level", "service", "timestamp"]
    )


# --- basic shape ----------------------------------------------------------


def test_storage_exposes_rows(storage: LogStorage, records: list[dict]) -> None:
    assert storage.rows() == records
    assert storage.has_hash_index("level")
    assert storage.has_hash_index("service")
    assert storage.has_ts_index()


def test_records_sorted_by_timestamp(records: list[dict]) -> None:
    timestamps = [r["timestamp"] for r in records]
    assert timestamps == sorted(timestamps)


def test_generate_logs_is_deterministic(time_range: TimeRange) -> None:
    a = generate_logs("partition-1", time_range, 50)
    b = generate_logs("partition-1", time_range, 50)
    assert a == b
    c = generate_logs("partition-2", time_range, 50)
    # Different partition id ⇒ different (deterministic) dataset.
    assert a != c


def test_generated_records_have_all_fields(records: list[dict]) -> None:
    for row in records[:20]:
        assert set(row.keys()) >= {
            "timestamp",
            "level",
            "service",
            "message",
            "status_code",
            "duration_ms",
        }
        assert row["level"] in {"INFO", "DEBUG", "WARN", "ERROR"}
        assert row["service"] in {"api", "auth", "db", "billing", "cache"}
        assert row["duration_ms"] >= 1
        assert isinstance(row["timestamp"], str)


def test_generated_pool_includes_timeout_message(records: list[dict]) -> None:
    assert any("timeout" in row["message"].lower() for row in records)


# --- hash indexes ---------------------------------------------------------


def test_filter_by_level_matches_brute_force(
    storage: LogStorage, records: list[dict]
) -> None:
    for level in ("INFO", "DEBUG", "WARN", "ERROR"):
        got = storage.filter_by_level(level)
        expected = {i for i, r in enumerate(records) if r["level"] == level}
        assert got == expected
        # All indices valid.
        for i in got:
            assert 0 <= i < len(records)


def test_filter_by_level_unknown_value_empty(storage: LogStorage) -> None:
    assert storage.filter_by_level("NOPE") == set()


def test_filter_by_service_matches_brute_force(
    storage: LogStorage, records: list[dict]
) -> None:
    for service in ("api", "auth", "db", "billing", "cache"):
        got = storage.filter_by_service(service)
        expected = {
            i for i, r in enumerate(records) if r["service"] == service
        }
        assert got == expected


# --- timestamp range index ------------------------------------------------


def test_filter_by_timestamp_range_full(
    storage: LogStorage, records: list[dict]
) -> None:
    got = storage.filter_by_timestamp_range(None, None)
    assert got == set(range(len(records)))


def test_filter_by_timestamp_range_low_bound(
    storage: LogStorage, records: list[dict]
) -> None:
    # Pick a point ~halfway through the sorted records.
    mid_ts = records[len(records) // 2]["timestamp"]
    got = storage.filter_by_timestamp_range(mid_ts, None)
    expected = {i for i, r in enumerate(records) if r["timestamp"] >= mid_ts}
    assert got == expected


def test_filter_by_timestamp_range_high_bound(
    storage: LogStorage, records: list[dict]
) -> None:
    mid_ts = records[len(records) // 2]["timestamp"]
    got = storage.filter_by_timestamp_range(None, mid_ts)
    expected = {i for i, r in enumerate(records) if r["timestamp"] <= mid_ts}
    assert got == expected


def test_filter_by_timestamp_range_inclusive_bounds(
    storage: LogStorage, records: list[dict]
) -> None:
    # Grab two distinct timestamps in ascending order.
    lo = records[10]["timestamp"]
    hi = records[-10]["timestamp"]
    got = storage.filter_by_timestamp_range(lo, hi)
    expected = {
        i for i, r in enumerate(records) if lo <= r["timestamp"] <= hi
    }
    assert got == expected
    # Check at least one of the boundary rows is present (inclusive).
    assert records[10]["timestamp"] in {records[i]["timestamp"] for i in got}


def test_filter_by_timestamp_range_outside_window_empty(
    storage: LogStorage,
) -> None:
    got = storage.filter_by_timestamp_range(
        "2027-01-01T00:00:00", "2027-12-31T00:00:00"
    )
    assert got == set()


# --- no-index fallback ----------------------------------------------------


def test_storage_works_without_any_indexes(records: list[dict]) -> None:
    storage = LogStorage(records=records, indexed_fields=[])
    assert not storage.has_hash_index("level")
    assert not storage.has_ts_index()

    # Lookups still produce correct results via the linear fallback.
    got = storage.filter_by_level("ERROR")
    expected = {i for i, r in enumerate(records) if r["level"] == "ERROR"}
    assert got == expected

    first_ts = records[0]["timestamp"]
    got = storage.filter_by_timestamp_range(first_ts, first_ts)
    expected = {i for i, r in enumerate(records) if r["timestamp"] == first_ts}
    assert got == expected
