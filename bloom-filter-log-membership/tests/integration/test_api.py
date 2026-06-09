"""Integration tests for the C8 core API: /logs/add, /logs/query, /stats.

Everything runs over the real ASGI app via the ``client`` fixture
(conftest), so each test exercises the full path: lifespan startup →
Pydantic validation → manager routing → metrics recording. The fixture is
function-scoped, so every test starts with fresh filters and zeroed
counters in an isolated DATA_DIR.

Spec wording is asserted with string literals (not the module constants) —
the exact response shapes ARE the contract under test. The latency bound
(< 50 ms for a µs-scale bloom op) is deliberately loose: it catches
"accidentally doing I/O on the hot path", not scheduler jitter on a busy
CI runner.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

ALL_LOG_TYPES = ("error_logs", "access_logs", "security_logs")


def _add(client: TestClient, log_type: str, log_key: str) -> dict:
    response = client.post(
        "/logs/add", json={"log_type": log_type, "log_key": log_key}
    )
    assert response.status_code == 200
    return response.json()


def _query(client: TestClient, log_type: str, log_key: str) -> dict:
    response = client.post(
        "/logs/query", json={"log_type": log_type, "log_key": log_key}
    )
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------- #
# spec workflow                                                          #
# ---------------------------------------------------------------------- #


def test_spec_workflow_add_then_query(client: TestClient) -> None:
    """The spec's canonical flow: add → query present → query absent."""
    added = _add(client, "error_logs", "req-1")
    assert added["status"] == "added"
    assert 0.0 <= added["processing_time_ms"] < 50.0

    present = _query(client, "error_logs", "req-1")
    assert present["might_exist"] is True
    assert present["confidence"] == "probably_exists"
    assert 0.0 <= present["processing_time_ms"] < 50.0

    absent = _query(client, "error_logs", "absent-xyz")
    assert absent["might_exist"] is False
    assert absent["confidence"] == "definitely_not_exist"
    assert 0.0 <= absent["processing_time_ms"] < 50.0


def test_response_key_sets_are_exact(client: TestClient) -> None:
    """The spec shapes carry exactly these keys — nothing extra leaks out."""
    added = _add(client, "error_logs", "shape-key")
    assert set(added) == {"status", "processing_time_ms"}

    queried = _query(client, "error_logs", "shape-key")
    assert set(queried) == {"might_exist", "confidence", "processing_time_ms"}


def test_duplicate_add_still_reports_added(client: TestClient) -> None:
    """Re-adding a key keeps the spec shape; dedup shows only in /stats."""
    first = _add(client, "error_logs", "dup-key")
    second = _add(client, "error_logs", "dup-key")
    assert first["status"] == second["status"] == "added"

    stats = client.get("/stats").json()["filters"]["error_logs"]
    assert stats["adds_total"] == 2  # both API calls counted...
    assert stats["elements_added"] == 1  # ...but only one distinct key admitted


def test_all_log_types_round_trip(client: TestClient) -> None:
    """Each configured filter answers for its own keys."""
    for log_type in ALL_LOG_TYPES:
        key = f"roundtrip-{log_type}"
        assert _add(client, log_type, key)["status"] == "added"

        present = _query(client, log_type, key)
        assert present["might_exist"] is True
        assert present["confidence"] == "probably_exists"

        absent = _query(client, log_type, f"never-added-{log_type}")
        assert absent["might_exist"] is False
        assert absent["confidence"] == "definitely_not_exist"


def test_cross_type_isolation_over_http(client: TestClient) -> None:
    """A key added to error_logs is not claimed by access_logs."""
    _add(client, "error_logs", "iso-key-1")
    answer = _query(client, "access_logs", "iso-key-1")
    assert answer["might_exist"] is False
    assert answer["confidence"] == "definitely_not_exist"


# ---------------------------------------------------------------------- #
# validation (FastAPI 422s before the manager is ever reached)           #
# ---------------------------------------------------------------------- #


def test_unknown_log_type_is_422(client: TestClient) -> None:
    for path in ("/logs/add", "/logs/query"):
        response = client.post(
            path, json={"log_type": "weird_logs", "log_key": "k"}
        )
        assert response.status_code == 422


def test_missing_log_key_is_422(client: TestClient) -> None:
    response = client.post("/logs/add", json={"log_type": "error_logs"})
    assert response.status_code == 422


def test_empty_log_key_is_422(client: TestClient) -> None:
    response = client.post(
        "/logs/add", json={"log_type": "error_logs", "log_key": ""}
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------- #
# /stats                                                                 #
# ---------------------------------------------------------------------- #


def test_stats_structure(client: TestClient) -> None:
    """Top-level shape: service id, uptime, all four filters, totals.

    The filter fleet is the three log types plus the C11 ``sessions``
    filter — a full /stats citizen even though it is not a ``log_type``.
    """
    response = client.get("/stats")
    assert response.status_code == 200
    stats = response.json()

    assert set(stats) == {"service", "uptime_seconds", "filters", "totals"}
    assert stats["service"] == "bloom-filter-log-membership"
    assert stats["uptime_seconds"] >= 0.0
    assert set(stats["filters"]) == set(ALL_LOG_TYPES) | {"sessions"}
    assert set(stats["totals"]) == {
        "elements_added",
        "adds_total",
        "queries_total",
        "memory_bytes",
        "memory_mb",
    }

    for per_filter in stats["filters"].values():
        # Every documented per-filter gauge is present from the first read.
        for key in (
            "elements_added",
            "capacity",
            "slice_count",
            "rotations",
            "previous_count",
            "memory_bytes",
            "memory_mb",
            "fill_ratio",
            "estimated_fp_rate",
            "target_fp_rate",
            "adds_total",
            "queries_total",
            "positives",
            "negatives",
            "avg_add_ms",
            "avg_query_ms",
            "p99_query_ms",
            "created_at",
            "generation_age_seconds",
        ):
            assert key in per_filter, f"missing per-filter stats key {key!r}"
        assert per_filter["estimated_fp_rate"] <= per_filter["target_fp_rate"]


def test_stats_counters_track_operations(client: TestClient) -> None:
    """5 distinct adds + 3 queries on error_logs land exactly in /stats."""
    for i in range(5):
        _add(client, "error_logs", f"stat-key-{i}")
    _query(client, "error_logs", "stat-key-0")  # positive
    _query(client, "error_logs", "stat-key-1")  # positive
    _query(client, "error_logs", "stat-absent")  # negative

    stats = client.get("/stats").json()
    error_logs = stats["filters"]["error_logs"]

    assert error_logs["adds_total"] == 5
    assert error_logs["elements_added"] == 5
    assert error_logs["queries_total"] == 3
    assert error_logs["positives"] == 2
    assert error_logs["negatives"] == 1
    assert error_logs["avg_add_ms"] > 0.0
    assert error_logs["avg_query_ms"] > 0.0
    assert error_logs["p99_query_ms"] > 0.0

    # The 1M-capacity error_logs filter pays ~1.7 MB for its slice-0 bitset
    # (tightened fp0 = 0.01 * 0.15) — anything near-zero would mean the
    # filter was silently mis-sized.
    assert error_logs["memory_mb"] > 1.0
    assert error_logs["memory_mb"] == round(
        error_logs["memory_bytes"] / (1024 * 1024), 3
    )

    # Untouched filters stay zeroed; totals are the cross-filter sums.
    assert stats["filters"]["access_logs"]["adds_total"] == 0
    assert stats["filters"]["security_logs"]["queries_total"] == 0
    assert stats["totals"]["elements_added"] == 5
    assert stats["totals"]["adds_total"] == 5
    assert stats["totals"]["queries_total"] == 3
    assert stats["totals"]["memory_bytes"] == sum(
        f["memory_bytes"] for f in stats["filters"].values()
    )
