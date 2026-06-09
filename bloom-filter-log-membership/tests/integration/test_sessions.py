"""Integration tests for the C11 session tracking endpoints: /sessions/*.

Everything rides the shared ``client`` fixture (real lifespan, tmp DATA_DIR
and SQLITE_PATH) at the REAL default sizing — the sessions filter is built
for 1M IDs at p=0.01, so the <2MB success criterion is asserted at the exact
scale Extended A specifies, not on a shrunken stand-in. The two
Extended-A success criteria pinned here:

* ``memory_under_2mb`` True with ``filter.memory_bytes < 2 MiB`` at 1M
  capacity (plus a direct ScalableBloomFilter construction cross-check);
* ``non_existent_correctly_identified_pct == 100.0`` from the
  with/without-bloom performance test, alongside a ≥40% storage-call
  reduction and sub-millisecond averages on BOTH paths. Raw ``speedup``
  is reported but deliberately not gated: against a warm in-process
  sqlite (~µs point-SELECTs) the filter's honest win is eliminating
  storage calls, not beating them on per-call latency.

Sessions are deliberately NOT a ``log_type``: the routing-boundary test
asserts ``/logs/*`` and ``/pipeline/*`` still 422 on ``"sessions"``.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

LOG_TYPE_FILTERS = ("error_logs", "access_logs", "security_logs")

INGEST_RESPONSE_KEYS = {"status", "duplicate", "processing_time_ms"}

QUERY_RESPONSE_KEYS = {
    "session_id",
    "might_exist",
    "found",
    "source",
    "storage_checked",
    "confidence",
    "processing_time_ms",
}

STATS_TOP_LEVEL_KEYS = {"filter", "memory_under_2mb", "pipeline", "ops"}

STATS_FILTER_KEYS = {
    "elements_added",
    "capacity",
    "slice_count",
    "rotations",
    "memory_bytes",
    "memory_mb",
    "estimated_fp_rate",
    "target_fp_rate",
}

#: Same per-filter tallies /pipeline/stats serves (sessions slice thereof).
STATS_PIPELINE_KEYS = {
    "storage_rows",
    "lookups",
    "bloom_negatives",
    "storage_skipped_pct",
    "storage_hits",
    "false_positives",
    "observed_fp_rate",
    "fallback_active",
    "fallback_lookups",
    "rotations_triggered",
}

PERFORMANCE_RESPONSE_KEYS = {
    "sessions_seeded",
    "lookups",
    "with_bloom_total_ms",
    "with_bloom_avg_ms",
    "without_bloom_total_ms",
    "without_bloom_avg_ms",
    "speedup",
    "storage_lookups_skipped",
    "storage_skipped_pct",
    "storage_calls_with_bloom",
    "storage_calls_without_bloom",
    "storage_calls_avoided_pct",
    "non_existent_correctly_identified_pct",
    "filter_memory_bytes",
    "filter_memory_mb",
    "processing_time_ms",
}

_2MB = 2 * 1024 * 1024


def _ingest(client: TestClient, session_id: str) -> dict:
    response = client.post("/sessions/ingest", json={"session_id": session_id})
    assert response.status_code == 200
    return response.json()


def _query(client: TestClient, session_id: str) -> dict:
    response = client.post("/sessions/query", json={"session_id": session_id})
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------- #
# ingest / query basics                                                  #
# ---------------------------------------------------------------------- #


def test_ingest_then_query_found(client: TestClient) -> None:
    """Hash-at-ingestion: an ingested session answers on the next query."""
    ingested = _ingest(client, "sess-abc")
    assert set(ingested) == INGEST_RESPONSE_KEYS
    assert ingested["status"] == "stored"
    assert ingested["duplicate"] is False
    assert ingested["processing_time_ms"] >= 0.0

    answer = _query(client, "sess-abc")
    assert set(answer) == QUERY_RESPONSE_KEYS
    assert answer["session_id"] == "sess-abc"
    assert answer["found"] is True
    assert answer["might_exist"] is True
    assert answer["confidence"] == "probably_exists"
    # A bloom positive is verified against the storage tier.
    assert answer["source"] == "storage"
    assert answer["storage_checked"] is True


def test_absent_session_short_circuits_storage(client: TestClient) -> None:
    """Bloom checked BEFORE storage: a filter negative never touches sqlite."""
    _ingest(client, "sess-present")

    answer = _query(client, "sess-never")
    assert answer["found"] is False
    assert answer["might_exist"] is False
    assert answer["confidence"] == "definitely_not_exist"
    assert answer["storage_checked"] is False  # the short-circuit proof
    assert answer["source"] == "bloom_negative"


def test_duplicate_ingest_flags_duplicate(client: TestClient) -> None:
    first = _ingest(client, "sess-dup")
    second = _ingest(client, "sess-dup")
    assert first["duplicate"] is False
    assert second["status"] == "stored"
    assert second["duplicate"] is True


# ---------------------------------------------------------------------- #
# /sessions/stats — shape and the <2MB criterion at REAL 1M sizing       #
# ---------------------------------------------------------------------- #


def test_sessions_stats_shape_and_sub_2mb_at_real_capacity(
    client: TestClient,
) -> None:
    """Default settings = 1M capacity: the spec's memory criterion, live.

    The client fixture runs the real defaults, so ``filter.memory_bytes``
    is the actual slice-0 bitset for 1M IDs at p=0.01 (~1.69 MB) — the
    assertion is the Extended-A success criterion itself, not a scaled-down
    proxy.
    """
    _ingest(client, "sess-stats-1")
    _query(client, "sess-stats-1")
    _query(client, "sess-stats-miss")

    response = client.get("/sessions/stats")
    assert response.status_code == 200
    stats = response.json()
    assert set(stats) == STATS_TOP_LEVEL_KEYS

    filter_block = stats["filter"]
    assert set(filter_block) == STATS_FILTER_KEYS
    assert filter_block["capacity"] >= 1_000_000
    assert filter_block["target_fp_rate"] == 0.01
    assert filter_block["elements_added"] >= 1
    assert filter_block["slice_count"] >= 1
    assert filter_block["rotations"] == 0

    # THE memory criterion: <2MB for the 1M-session generation.
    assert stats["memory_under_2mb"] is True
    assert filter_block["memory_bytes"] < _2MB
    assert filter_block["memory_mb"] == round(
        filter_block["memory_bytes"] / (1024 * 1024), 3
    )

    pipeline_block = stats["pipeline"]
    assert set(pipeline_block) == STATS_PIPELINE_KEYS
    assert pipeline_block["storage_rows"] == 1
    assert pipeline_block["lookups"] == 2
    assert pipeline_block["bloom_negatives"] == 1
    assert pipeline_block["storage_hits"] == 1

    ops_block = stats["ops"]
    assert ops_block["adds_total"] == 1
    assert ops_block["queries_total"] >= 1  # the bloom-consulted lookup(s)


def test_sessions_filter_sizing_is_between_1_and_2_mb() -> None:
    """Direct construction: 1M @ p=0.01 lands at ~1.69MB (no app needed).

    Slice 0 gets the tightened budget 0.01 × 0.15 = 0.0015 → m ≈ 13.53 Mbit
    ≈ 1.69 MB: under the 2 MiB criterion, and over 1 MiB (anything smaller
    would mean the SBF budget math silently changed).
    """
    from src.scalable import ScalableBloomFilter

    sbf = ScalableBloomFilter(initial_capacity=1_000_000, target_fp_rate=0.01)
    assert 1 * 1024 * 1024 < sbf.memory_bytes < _2MB


# ---------------------------------------------------------------------- #
# routing: the 4th filter joins the fleet, but never the log_type API    #
# ---------------------------------------------------------------------- #


def test_sessions_appears_in_global_stats_and_pipeline_stats(
    client: TestClient,
) -> None:
    """The sessions filter is a full fleet member in both stats views."""
    _ingest(client, "sess-global")

    filters = client.get("/stats").json()["filters"]
    assert set(filters) == set(LOG_TYPE_FILTERS) | {"sessions"}
    assert filters["sessions"]["elements_added"] == 1

    pipeline_stats = client.get("/pipeline/stats").json()
    assert set(pipeline_stats) == set(LOG_TYPE_FILTERS) | {"sessions", "_totals"}
    assert pipeline_stats["sessions"]["storage_rows"] == 1


def test_log_type_endpoints_reject_sessions(client: TestClient) -> None:
    """"sessions" is not a log_type: only /sessions/* serves session traffic."""
    for path in (
        "/logs/add",
        "/logs/query",
        "/pipeline/ingest",
        "/pipeline/lookup",
    ):
        response = client.post(
            path, json={"log_type": "sessions", "log_key": "sess-route"}
        )
        assert response.status_code == 422, path


# ---------------------------------------------------------------------- #
# performance test — with vs without the filter (Extended A #5)          #
# ---------------------------------------------------------------------- #


def test_performance_test_reduces_storage_calls(
    client: TestClient,
) -> None:
    """The with/without benchmark: fewer storage calls, µs-fast, 100% correct.

    Deliberately NOT gated on raw speedup: against a warm in-process
    sqlite, point-PK SELECTs cost about as much as a Python-level filter
    probe, so the filter cannot honestly win on per-call latency here. Its
    genuine, transferable win is structural — every bloom negative
    eliminates a storage call outright — and both paths must stay far
    under the spec's <1 ms query criterion.
    """
    response = client.post(
        "/sessions/performance-test?sessions=800&lookups=400"
    )
    assert response.status_code == 200
    report = response.json()
    assert set(report) == PERFORMANCE_RESPONSE_KEYS

    assert report["sessions_seeded"] == 800
    assert report["lookups"] == 400
    assert report["with_bloom_total_ms"] > 0.0
    assert report["without_bloom_total_ms"] > 0.0

    # Success criterion: EVERY non-existent session correctly identified —
    # bloom negatives are proofs, and bloom false positives get corrected
    # by the storage verification, so nothing short of a bug yields <100.
    assert report["non_existent_correctly_identified_pct"] == 100.0

    # The structural win: the filter eliminates the storage call on every
    # bloom negative. At the 50/50 mix that is ~50% of all calls (>=40
    # allows for the rare bloom false positive).
    assert report["storage_calls_avoided_pct"] >= 40.0
    assert (
        report["storage_calls_with_bloom"]
        < report["storage_calls_without_bloom"]
    )
    assert report["storage_calls_without_bloom"] == 400  # one per probe

    # Both paths meet the spec's <1ms criterion with µs-scale headroom;
    # raw speedup is reported truthfully in the payload but not gated.
    assert report["with_bloom_avg_ms"] < 1.0
    assert report["without_bloom_avg_ms"] < 1.0

    # The same mechanism, as probe-level accounting: ~half the probes are
    # absent and nearly all of them short-circuit at the filter.
    assert report["storage_skipped_pct"] > 40.0
    assert report["storage_lookups_skipped"] > 160  # 40% of 400 probes

    # The benchmark runs on the live 1M filter: memory stays sub-2MB.
    assert report["filter_memory_bytes"] < _2MB

    # Seeding was real two-tier traffic: rows landed in the storage tier.
    stats = client.get("/sessions/stats").json()
    assert stats["pipeline"]["storage_rows"] >= 800
    assert stats["filter"]["elements_added"] >= 800
    assert stats["memory_under_2mb"] is True


def test_performance_test_validates_bounds(client: TestClient) -> None:
    """sessions has a 100-floor (and lookups a 10-floor): 422 below them."""
    assert (
        client.post("/sessions/performance-test?sessions=10").status_code == 422
    )
    assert (
        client.post(
            "/sessions/performance-test?sessions=200&lookups=5"
        ).status_code
        == 422
    )


# ---------------------------------------------------------------------- #
# validation                                                             #
# ---------------------------------------------------------------------- #


def test_empty_or_missing_session_id_is_422(client: TestClient) -> None:
    for path in ("/sessions/ingest", "/sessions/query"):
        assert client.post(path, json={"session_id": ""}).status_code == 422
        assert client.post(path, json={}).status_code == 422
