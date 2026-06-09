"""Integration tests for the C9 demo endpoints: /demo/populate, /demo/performance-test.

Everything runs over the real ASGI app via the function-scoped ``client``
fixture (conftest), so each test starts from fresh filters and zeroed
counters in an isolated DATA_DIR; before/after /stats snapshots are still
taken where deltas are asserted, so the tests stay correct even if fixture
scoping ever changes.

The populate tests pin the exact spec response shape
(``{"status": "completed", "records_added": N}`` and nothing more) and
verify the seeding is REAL by watching /stats move. The performance-test
gates are deliberately conservative CI floors: speedup > 10 (typically
100x+ at default sizes), bloom_avg_ms < 0.05 (a µs-scale op given a 50µs
allowance), FP count within 5% of absent probes (expectation at p=0.01 is
~1%). They catch "benchmark is broken", not scheduler jitter on a busy
runner. The small-run dataset is sized at 8000 on purpose: a 2000-item run
once measured speedup 5.99 on a cold Docker first run (24+ on re-run) —
linear cost scales with dataset size while bloom stays constant, so a
bigger dataset widens the margin under the SAME gate instead of loosening
the gate.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

ALL_LOG_TYPES = ("error_logs", "access_logs", "security_logs")

#: Exact response key set of /demo/performance-test — the C9 contract.
PERFORMANCE_TEST_KEYS = {
    "dataset_size",
    "lookups",
    "bloom_total_ms",
    "bloom_avg_ms",
    "linear_total_ms",
    "linear_avg_ms",
    "speedup_vs_linear",
    "bloom_memory_bytes",
    "keys_memory_bytes_estimate",
    "memory_ratio",
    "false_positives_observed",
    "processing_time_ms",
}


def _stats(client: TestClient) -> dict:
    response = client.get("/stats")
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------- #
# /demo/populate                                                         #
# ---------------------------------------------------------------------- #


def test_populate_default_count_and_exact_spec_shape(client: TestClient) -> None:
    """No query param → 10000 records; response is exactly the spec sample."""
    before = _stats(client)["totals"]["adds_total"]

    response = client.post("/demo/populate")
    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "completed", "records_added": 10_000}
    assert set(body) == {"status", "records_added"}  # nothing extra leaks out

    after = _stats(client)["totals"]["adds_total"]
    assert after - before == 10_000  # the seeding really went through the manager


def test_populate_custom_count_round_robins_across_filters(client: TestClient) -> None:
    """count=300 → exactly 100 adds land in each of the three filters."""
    before = _stats(client)["filters"]

    response = client.post("/demo/populate", params={"count": 300})
    assert response.status_code == 200
    assert response.json()["records_added"] == 300

    after = _stats(client)["filters"]
    for log_type in ALL_LOG_TYPES:
        delta = after[log_type]["adds_total"] - before[log_type]["adds_total"]
        assert delta == 100, f"{log_type} got {delta} adds, expected 100"


def test_populate_count_validation(client: TestClient) -> None:
    """The 1..1_000_000 bounds are enforced by FastAPI before any work runs."""
    assert client.post("/demo/populate", params={"count": 0}).status_code == 422
    assert (
        client.post("/demo/populate", params={"count": 1_000_001}).status_code == 422
    )


def test_repeat_populate_adds_new_records(client: TestClient) -> None:
    """Two populate calls seed DISTINCT keys (fresh nonce per call)."""
    before = _stats(client)["totals"]["elements_added"]

    for _ in range(2):
        response = client.post("/demo/populate", params={"count": 90})
        assert response.status_code == 200
        assert response.json()["records_added"] == 90

    grown = _stats(client)["totals"]["elements_added"] - before
    # Two 90-key batches under different nonces → ~180 distinct elements.
    # The floor tolerates the astronomically-rare-at-this-fill case of a new
    # key reading as a duplicate (all k bits colliding); an exact ==180
    # would be flaky-by-design. A nonce reuse bug would land at ~90.
    assert 175 <= grown <= 180


# ---------------------------------------------------------------------- #
# /demo/performance-test                                                 #
# ---------------------------------------------------------------------- #


def test_performance_test_small_run_meets_gates(client: TestClient) -> None:
    """Small benchmark run: exact shape + speed/memory/FP gates hold."""
    response = client.post(
        "/demo/performance-test", params={"lookups": 300, "dataset_size": 8000}
    )
    assert response.status_code == 200
    body = response.json()

    assert set(body) == PERFORMANCE_TEST_KEYS
    assert body["dataset_size"] == 8000
    assert body["lookups"] == 300

    # Speed: conservative CI floor (typically 100-1000x at default sizes;
    # an 8000-item linear scan keeps even a cold-cache Docker run well
    # clear of the floor — 2000 items once flaked at 5.99x, see module
    # docstring). bloom_avg_ms stays unchanged: bloom lookups are
    # constant-time regardless of dataset size.
    assert body["speedup_vs_linear"] > 10
    assert body["bloom_avg_ms"] < 0.05  # µs-scale op, 50µs allowance
    assert body["linear_total_ms"] > body["bloom_total_ms"]
    assert body["bloom_total_ms"] > 0.0
    assert body["processing_time_ms"] > 0.0

    # Memory: bloom bitset under 5% of the (conservative) raw key bytes.
    assert body["bloom_memory_bytes"] > 0
    assert body["keys_memory_bytes_estimate"] > 0
    assert body["memory_ratio"] < 0.05
    assert body["memory_ratio"] == round(
        body["bloom_memory_bytes"] / body["keys_memory_bytes_estimate"], 6
    )

    # False positives: 150 absent probes at p=0.01 → expectation ~1.5;
    # 5% cap = 7.5, rounded up to 8 (P[X > 8] for Poisson(1.5) ≈ 2e-5).
    assert 0 <= body["false_positives_observed"] <= 8


def test_performance_test_param_validation(client: TestClient) -> None:
    """lookups >= 1 and dataset_size >= 100 are enforced as 422s."""
    assert (
        client.post("/demo/performance-test", params={"lookups": 0}).status_code
        == 422
    )
    assert (
        client.post(
            "/demo/performance-test", params={"dataset_size": 99}
        ).status_code
        == 422
    )


def test_performance_test_does_not_pollute_live_stats(client: TestClient) -> None:
    """The benchmark is self-contained: live filters and metrics never move."""
    # Seed a little real traffic first so "unchanged" is a meaningful claim.
    add = client.post(
        "/logs/add", json={"log_type": "error_logs", "log_key": "real-key-1"}
    )
    assert add.status_code == 200

    before = _stats(client)

    response = client.post(
        "/demo/performance-test", params={"lookups": 100, "dataset_size": 200}
    )
    assert response.status_code == 200

    after = _stats(client)
    assert (
        after["totals"]["elements_added"] == before["totals"]["elements_added"]
    )
    assert after["totals"]["adds_total"] == before["totals"]["adds_total"]
    assert after["totals"]["queries_total"] == before["totals"]["queries_total"]
    for log_type in ALL_LOG_TYPES:
        assert (
            after["filters"][log_type]["queries_total"]
            == before["filters"][log_type]["queries_total"]
        )
