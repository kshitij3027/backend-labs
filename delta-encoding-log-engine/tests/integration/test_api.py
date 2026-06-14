"""TestClient integration tests for the FastAPI REST surface (Commit 9).

Drives the real wired app (``app.main:app``) through ``fastapi.testclient.TestClient``
under a ``with`` block so the :func:`app.main.lifespan` runs and builds the shared
``app.state`` graph (settings + metrics + segment store). Because that graph is a
process-wide singleton on the ``app`` object, every test starts from ``POST /api/reset``
(via the ``client`` fixture) so the store and the metrics registry — including the
``system.errors`` counter — are zeroed for isolation.

The full data-flow contract under test is: ``generate`` → ``compress`` → ``reconstruct``
round-trips with byte-for-byte fidelity (``entries_equal``), random access returns the
right entry with its nearest keyframe, paging matches the generated slice, the
≥60% ``delta_reduction`` storage claim holds on a churny 1000-entry batch, the three-section
``/api/stats`` shape is well-formed, and — crucially — client errors (404 / 422 / 400)
never bump ``system.errors``.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.codec import entries_equal
from app.main import app


@pytest.fixture
def client():
    """Yield a TestClient with lifespan active; reset engine state before each test.

    The ``with TestClient(app)`` block triggers startup/shutdown so ``app.state`` is
    fully built. ``POST /api/reset`` clears the store + metrics (including the error
    counter and uptime) so tests don't leak generated batches or error counts into
    one another despite sharing the singleton app state.
    """
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def _generate(client, count, seed):
    """POST /api/generate and return (response, generated_logs); asserts 200 + count."""
    resp = client.post("/api/generate", json={"count": count, "seed": seed})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == count
    assert len(body["logs"]) == count
    return resp, body["logs"]


# --------------------------------------------------------------------------- #
# Health.
# --------------------------------------------------------------------------- #
def test_health_ok(client):
    """GET /health returns 200 with the healthy status payload."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


# --------------------------------------------------------------------------- #
# Full round-trip: generate -> compress -> reconstruct (verify fidelity).
# --------------------------------------------------------------------------- #
def test_full_round_trip_fidelity(client):
    """generate(300) -> compress(use_generated) -> reconstruct(verify) round-trips exactly."""
    _, generated = _generate(client, count=300, seed=7)

    # Compress the server's last-generated batch.
    cresp = client.post("/api/compress", json={"use_generated": True})
    assert cresp.status_code == 200, cresp.text
    cbody = cresp.json()
    assert cbody["count"] == 300
    # Byte-accounting fields are present.
    for key in ("delta_reduction", "compression_ratio", "keyframe_count", "delta_count"):
        assert key in cbody, f"missing {key} in compress response: {cbody}"
    # keyframe_interval defaults to 100 -> 300 entries => 3 keyframes, 297 deltas.
    assert cbody["keyframe_count"] >= 1
    assert cbody["keyframe_count"] + cbody["delta_count"] == 300

    # Reconstruct the whole batch and verify fidelity against the stored raw.
    rresp = client.post("/api/reconstruct", json={"verify": True})
    assert rresp.status_code == 200, rresp.text
    rbody = rresp.json()
    assert rbody["count"] == 300
    assert rbody["fidelity_ok"] is True

    # Independently confirm: reconstructed logs are element-wise canonically equal
    # to the logs returned by /api/generate (not just to the server's own raw copy).
    reconstructed = rbody["logs"]
    assert len(reconstructed) == len(generated) == 300
    assert all(entries_equal(a, b) for a, b in zip(reconstructed, generated))


# --------------------------------------------------------------------------- #
# Random access: GET /api/logs/{index}.
# --------------------------------------------------------------------------- #
def test_random_access_single_entry(client):
    """GET /api/logs/{i} returns entry == generated[i] with a nearest_keyframe_index."""
    _, generated = _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    for idx in (0, 150, 299):
        resp = client.get(f"/api/logs/{idx}")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "entry" in body
        assert entries_equal(body["entry"], generated[idx]), f"mismatch at index {idx}"
        assert "nearest_keyframe_index" in body
        # Nearest keyframe is the segment start (keyframe_interval default = 100).
        assert body["nearest_keyframe_index"] == (idx // 100) * 100


def test_random_access_out_of_range_404(client):
    """GET /api/logs/{i} for an out-of-range index returns 404 (a client error)."""
    _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    resp = client.get("/api/logs/99999")
    assert resp.status_code == 404


# --------------------------------------------------------------------------- #
# Paging: GET /api/logs?offset=&limit=.
# --------------------------------------------------------------------------- #
def test_paging_matches_generated_slice(client):
    """GET /api/logs?offset=10&limit=20 returns 20 logs == generated[10:30], total=300."""
    _, generated = _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    resp = client.get("/api/logs", params={"offset": 10, "limit": 20})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 300
    page = body["logs"]
    assert len(page) == 20
    expected = generated[10:30]
    assert all(entries_equal(a, b) for a, b in zip(page, expected))


# --------------------------------------------------------------------------- #
# Compress an explicitly-supplied batch (use_generated=false).
# --------------------------------------------------------------------------- #
def test_compress_explicit_logs_round_trips(client):
    """POST /api/compress with explicit logs round-trips via reconstruct(verify)."""
    batch = [
        {"ts": 1000, "level": "INFO", "service": "api", "code": 200},
        {"ts": 1001, "level": "INFO", "service": "api", "code": 200},
        {"ts": 1002, "level": "ERROR", "service": "api", "code": 500, "error": "boom"},
        {"ts": 1003, "level": "INFO", "service": "api", "code": 200},
    ]
    cresp = client.post(
        "/api/compress", json={"use_generated": False, "logs": batch}
    )
    assert cresp.status_code == 200, cresp.text
    cbody = cresp.json()
    assert cbody["count"] == len(batch)

    rresp = client.post("/api/reconstruct", json={"verify": True})
    assert rresp.status_code == 200, rresp.text
    rbody = rresp.json()
    assert rbody["count"] == len(batch)
    assert rbody["fidelity_ok"] is True
    assert all(entries_equal(a, b) for a, b in zip(rbody["logs"], batch))


# --------------------------------------------------------------------------- #
# Reduction sanity: >=60% delta_reduction on a churny 1000-entry batch.
# --------------------------------------------------------------------------- #
def test_delta_reduction_at_least_60_percent(client):
    """After generate(1000)+compress, storage delta_reduction & savings >= 60%."""
    _generate(client, count=1000, seed=11)
    cresp = client.post("/api/compress", json={"use_generated": True})
    assert cresp.status_code == 200, cresp.text
    assert cresp.json()["delta_reduction"] >= 60.0

    sresp = client.get("/api/stats")
    assert sresp.status_code == 200, sresp.text
    storage = sresp.json()["storage"]
    assert storage["delta_reduction"] >= 60.0
    assert storage["storage_savings_percent"] >= 60.0


# --------------------------------------------------------------------------- #
# Stats shape + error-counter discipline.
# --------------------------------------------------------------------------- #
def test_stats_shape_and_errors_zero_after_normal_calls(client):
    """/api/stats has storage/performance/system; system.errors == 0 after normal flow."""
    _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})
    client.post("/api/reconstruct", json={"verify": True})
    client.get("/api/logs/150")
    client.get("/api/logs", params={"offset": 0, "limit": 50})

    resp = client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Top-level three-section shape.
    assert "storage" in body
    assert "performance" in body
    assert "system" in body

    # No unexpected (500) errors occurred during the normal flow.
    assert body["system"]["errors"] == 0

    # Performance block exposes per-op detail + the convenience gate keys.
    perf = body["performance"]
    assert "operations" in perf
    assert "reconstruct_p99_ms" in perf
    assert "reconstruct_p50_ms" in perf
    assert "compress_throughput_eps" in perf
    # The ops we exercised should be present.
    assert "compress" in perf["operations"]
    assert "reconstruct" in perf["operations"]


def test_client_errors_do_not_increment_system_errors(client):
    """A 404 and a 422 are client errors and must NOT bump system.errors."""
    # Set up a valid batch first so the 404 path is genuinely out-of-range.
    _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    # 404: out-of-range random access.
    r404 = client.get("/api/logs/99999")
    assert r404.status_code == 404

    # 422: validation failure (count below the ge=1 bound).
    r422 = client.post("/api/generate", json={"count": 0})
    assert r422.status_code == 422

    resp = client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    assert resp.json()["system"]["errors"] == 0


# --------------------------------------------------------------------------- #
# Validation + empty-store behaviour.
# --------------------------------------------------------------------------- #
def test_generate_count_too_small_422(client):
    """POST /api/generate {"count": 0} fails validation (ge=1) with 422."""
    resp = client.post("/api/generate", json={"count": 0})
    assert resp.status_code == 422


def test_generate_count_too_large_422(client):
    """POST /api/generate {"count": 100001} fails validation (le=100000) with 422."""
    resp = client.post("/api/generate", json={"count": 100001})
    assert resp.status_code == 422


def test_compress_no_logs_without_generated_is_client_error(client):
    """POST /api/compress {use_generated:false} with no logs -> 400 (or 422)."""
    resp = client.post("/api/compress", json={"use_generated": False})
    assert resp.status_code in (400, 422), resp.text


def test_empty_store_behaves_sanely_and_keeps_errors_zero(client):
    """Right after reset (empty store): reconstruct -> empty, logs page -> empty,
    logs/0 -> 404; and none of it touches system.errors."""
    # Fresh reset (the fixture already reset, but be explicit about the empty state).
    client.post("/api/reset")

    # reconstruct on empty store: returns an empty list, count 0 (does not crash).
    rresp = client.post("/api/reconstruct", json={"verify": True})
    assert rresp.status_code == 200, rresp.text
    rbody = rresp.json()
    assert rbody["count"] == 0
    assert rbody["logs"] == []

    # logs page on empty store: empty list, total 0.
    presp = client.get("/api/logs", params={"offset": 0, "limit": 50})
    assert presp.status_code == 200, presp.text
    pbody = presp.json()
    assert pbody["logs"] == []
    assert pbody["total"] == 0

    # logs/0 on empty store: 404 (nothing compressed yet).
    iresp = client.get("/api/logs/0")
    assert iresp.status_code == 404

    # None of the above were unexpected failures.
    sresp = client.get("/api/stats")
    assert sresp.status_code == 200, sresp.text
    assert sresp.json()["system"]["errors"] == 0


# --------------------------------------------------------------------------- #
# Reset.
# --------------------------------------------------------------------------- #
def test_reset_clears_store(client):
    """After POST /api/reset, /api/stats storage.count == 0."""
    _generate(client, count=300, seed=7)
    client.post("/api/compress", json={"use_generated": True})

    # Confirm there is something stored, then reset.
    pre = client.get("/api/stats").json()
    assert pre["storage"]["count"] == 300

    resp = client.post("/api/reset")
    assert resp.status_code == 200, resp.text

    post = client.get("/api/stats").json()
    assert post["storage"]["count"] == 0
