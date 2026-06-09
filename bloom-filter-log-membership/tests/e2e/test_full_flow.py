"""In-process end-to-end scenario: one user's walk through the whole API.

This is the ``make test-e2e`` counterpart to ``scripts/verify_e2e.py``: the
same add → query → populate → benchmark → pipeline → sessions journey, but
through the in-process ``client`` fixture (real lifespan, tmp DATA_DIR and
SQLITE_PATH) so it runs without containers. One deliberately readable
scenario asserting each step's KEY invariant — exhaustive per-endpoint
coverage lives in ``tests/integration``; the cross-container truth (real
uvicorn, compose network, dashboard process) lives in the verifier script.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

LOG_TYPES = ("error_logs", "access_logs", "security_logs")
ALL_FILTERS = LOG_TYPES + ("sessions",)

#: Scenario sizing: small enough to stay fast, big enough to be meaningful.
POPULATE_COUNT = 300  # divisible by 3 → exactly 100 per log-type filter
#: Every manager.add this scenario performs (uniquely-keyed, so elements track
#: adds): 3 roundtrip keys + the populate batch + 1 pipeline ingest + 1
#: session ingest. The perf benchmark must contribute ZERO (non-pollution).
EXPECTED_ADDS_TOTAL = 3 + POPULATE_COUNT + 1 + 1


def test_full_user_flow(client: TestClient) -> None:
    # --- 1) add + query roundtrip on every log type ----------------------
    for log_type in LOG_TYPES:
        body = {"log_type": log_type, "log_key": f"flow-{log_type}-key"}
        added = client.post("/logs/add", json=body)
        assert added.status_code == 200
        assert added.json()["status"] == "added"

        answer = client.post("/logs/query", json=body).json()
        assert answer["might_exist"] is True
        assert answer["confidence"] == "probably_exists"

    # Routing isolation: error_logs' key never pollutes access_logs ...
    cross = client.post(
        "/logs/query",
        json={"log_type": "access_logs", "log_key": "flow-error_logs-key"},
    ).json()
    assert cross["might_exist"] is False
    # ... and a never-added key is a definite no (zero false negatives).
    absent = client.post(
        "/logs/query",
        json={"log_type": "error_logs", "log_key": "flow-never-added"},
    ).json()
    assert absent["confidence"] == "definitely_not_exist"
    # An unknown log_type never reaches the manager.
    assert (
        client.post(
            "/logs/add", json={"log_type": "bogus_logs", "log_key": "x"}
        ).status_code
        == 422
    )

    # --- 2) bulk populate: exact spec shape, counters move exactly -------
    populated = client.post("/demo/populate", params={"count": POPULATE_COUNT})
    assert populated.status_code == 200
    assert populated.json() == {
        "status": "completed",
        "records_added": POPULATE_COUNT,
    }

    # --- 3) bloom-vs-linear benchmark: gates hold, live stats untouched --
    before = client.get("/stats").json()["totals"]
    perf = client.post(
        "/demo/performance-test", params={"lookups": 300, "dataset_size": 8000}
    ).json()
    assert perf["speedup_vs_linear"] > 10  # proven-stable CI floor at 8k items
    assert perf["memory_ratio"] < 0.05
    after = client.get("/stats").json()["totals"]
    assert after["adds_total"] == before["adds_total"]  # non-pollution
    assert after["queries_total"] == before["queries_total"]

    # --- 4) two-tier pipeline: hit verified, miss skips storage ----------
    pipe_body = {"log_type": "security_logs", "log_key": "flow-pipe-key"}
    ingested = client.post("/pipeline/ingest", json=pipe_body).json()
    assert ingested["status"] == "stored"
    assert ingested["bloom_updated"] is True

    hit = client.post("/pipeline/lookup", json=pipe_body).json()
    assert hit["found"] is True
    assert hit["source"] == "storage"
    miss = client.post(
        "/pipeline/lookup",
        json={"log_type": "security_logs", "log_key": "flow-pipe-absent"},
    ).json()
    assert miss["found"] is False
    assert miss["storage_checked"] is False  # the expensive tier was skipped
    assert miss["source"] == "bloom_negative"

    # --- 5) sessions: hash-at-ingestion, bloom-first query, <2MB ---------
    assert (
        client.post("/sessions/ingest", json={"session_id": "flow-sess"}).json()[
            "status"
        ]
        == "stored"
    )
    found = client.post("/sessions/query", json={"session_id": "flow-sess"}).json()
    assert found["found"] is True
    assert found["confidence"] == "probably_exists"
    sess_absent = client.post(
        "/sessions/query", json={"session_id": "flow-sess-absent"}
    ).json()
    assert sess_absent["found"] is False
    assert sess_absent["storage_checked"] is False

    sess_stats = client.get("/sessions/stats").json()
    assert sess_stats["memory_under_2mb"] is True
    assert sess_stats["filter"]["memory_bytes"] < 2 * 1024 * 1024
    assert sess_stats["pipeline"]["storage_rows"] == 1

    # --- 6) /stats coherence over the whole journey ----------------------
    stats = client.get("/stats").json()
    filters = stats["filters"]
    assert set(filters) == set(ALL_FILTERS)
    totals = stats["totals"]
    assert totals["adds_total"] == EXPECTED_ADDS_TOTAL  # metrics count every add
    # Every key was unique, so elements track adds (<= tolerates the
    # astronomically-rare all-bits-collide duplicate at this fill level).
    assert EXPECTED_ADDS_TOTAL - 3 <= totals["elements_added"] <= EXPECTED_ADDS_TOTAL
    # Totals are exactly the sum of their per-filter parts.
    for key in ("elements_added", "adds_total", "queries_total", "memory_bytes"):
        assert totals[key] == sum(f[key] for f in filters.values()), key
    assert all(f["memory_bytes"] > 0 for f in filters.values())
    assert stats["uptime_seconds"] >= 0
