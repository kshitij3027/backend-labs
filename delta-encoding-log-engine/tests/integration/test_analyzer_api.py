"""TestClient integration tests for the read-only analyzer section (Commit 14).

Verifies that the thin adaptive recommender is wired into the API additively and
remains purely advisory:

* ``GET /api/stats`` grows a top-level ``analyzer`` section after generate+compress,
  with the expected snapshot keys and ``samples > 0``.
* The pre-existing ``storage`` / ``performance`` / ``system`` sections are STILL
  present and well-formed (the ``compose_stats`` signature change didn't break them).
* The interval recommendation moves the right way *through the API*: a low-churn
  batch yields a LARGER ``recommended_keyframe_interval`` than a high-churn batch.
* ``POST /api/reset`` clears the analyzer window (``samples == 0``).
* **Read-only guarantee:** the analyzer does NOT alter compression — the
  ``/api/compress`` ``delta_reduction`` for a fixed seeded batch is byte-identical to a
  direct :class:`~app.store.SegmentStore` compression of the same batch (the store that
  the live app builds, but with no analyzer attached).

Shares the singleton-app pattern of ``test_api.py``: each test resets engine state first.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.encoders import EncoderConfig
from app.generator import generate_logs
from app.main import app
from app.store import SegmentStore

# The snapshot keys the analyzer section must always carry (mirrors PatternAnalyzer.snapshot).
_ANALYZER_KEYS = {
    "observed_churn",
    "recommended_keyframe_interval",
    "current_keyframe_interval",
    "recommended_mode",
    "mode",
    "window",
    "samples",
}


@pytest.fixture
def client():
    """Yield a TestClient with lifespan active; reset engine state before each test."""
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


# --------------------------------------------------------------------------- #
# analyzer section appears and is well-formed.
# --------------------------------------------------------------------------- #
def test_stats_has_analyzer_section_after_compress(client):
    """After generate+compress, /api/stats carries a populated analyzer section."""
    client.post("/api/generate", json={"count": 300, "seed": 7})
    client.post("/api/compress", json={"use_generated": True})

    body = client.get("/api/stats").json()
    assert "analyzer" in body, f"no analyzer section: {body.keys()}"
    analyzer = body["analyzer"]
    assert set(analyzer) == _ANALYZER_KEYS
    # The just-compressed batch fed the window, so it has samples.
    assert analyzer["samples"] > 0
    # Modes serialize as plain strings.
    assert isinstance(analyzer["recommended_mode"], str)
    assert isinstance(analyzer["mode"], str)
    assert 0.0 <= analyzer["observed_churn"] <= 1.0


def test_other_stats_sections_still_present_and_well_formed(client):
    """storage / performance / system are intact after the compose_stats signature change."""
    client.post("/api/generate", json={"count": 300, "seed": 7})
    client.post("/api/compress", json={"use_generated": True})
    client.get("/api/logs/150")  # exercise a reconstruct so performance has data

    body = client.get("/api/stats").json()

    # All four top-level sections present.
    for section in ("storage", "performance", "system", "analyzer"):
        assert section in body, f"missing {section}: {list(body)}"

    # storage byte accounting intact.
    storage = body["storage"]
    assert storage["count"] == 300
    assert "delta_reduction" in storage
    assert "storage_savings_percent" in storage

    # performance intact (per-op + gate keys + cache fold).
    perf = body["performance"]
    assert "operations" in perf
    assert "reconstruct_p99_ms" in perf
    assert "compress_throughput_eps" in perf
    assert "cache" in perf

    # system intact (health / errors / uptime), no unexpected errors.
    system = body["system"]
    assert system["status"] == "healthy"
    assert system["errors"] == 0
    assert "uptime_seconds" in system


# --------------------------------------------------------------------------- #
# Recommendation moves the right way through the API.
# --------------------------------------------------------------------------- #
def test_low_churn_recommends_larger_interval_than_high_churn_via_api(client):
    """LOW-churn batch -> larger recommended_keyframe_interval than HIGH-churn (through API)."""
    # LOW churn run.
    client.post("/api/reset")
    client.post("/api/generate", json={"count": 300, "seed": 1, "churn": 0.0})
    client.post("/api/compress", json={"use_generated": True})
    low = client.get("/api/stats").json()["analyzer"]["recommended_keyframe_interval"]

    # HIGH churn run (independent window after reset).
    client.post("/api/reset")
    client.post("/api/generate", json={"count": 300, "seed": 1, "churn": 1.0})
    client.post("/api/compress", json={"use_generated": True})
    high = client.get("/api/stats").json()["analyzer"]["recommended_keyframe_interval"]

    assert low > high, f"expected LOW({low}) > HIGH({high}) recommended interval"


# --------------------------------------------------------------------------- #
# reset clears the analyzer window.
# --------------------------------------------------------------------------- #
def test_reset_clears_analyzer_samples(client):
    """POST /api/reset zeroes the analyzer window (samples == 0)."""
    client.post("/api/generate", json={"count": 200, "seed": 3})
    client.post("/api/compress", json={"use_generated": True})
    assert client.get("/api/stats").json()["analyzer"]["samples"] > 0

    client.post("/api/reset")
    assert client.get("/api/stats").json()["analyzer"]["samples"] == 0


# --------------------------------------------------------------------------- #
# Read-only guarantee: the analyzer does not alter compression.
# --------------------------------------------------------------------------- #
def test_analyzer_does_not_change_compression_output(client):
    """/api/compress delta_reduction matches a direct SegmentStore compress (no analyzer).

    The live app observes the batch in the analyzer right after compressing it. If that
    observation leaked into the encoder, the byte accounting would differ from a plain
    store that never saw an analyzer. We build the SAME store the app's lifespan builds
    (keyframe_interval=100, baseline="previous", all-on encoder, gzip off — the configured
    defaults) and compress the identical seeded batch directly; the delta_reduction must be
    byte-identical, proving the analyzer changed nothing about how the batch was encoded.
    """
    batch = generate_logs(300, seed=42, churn=0.2, schema_width=8)

    # Through the API (the analyzer observes this batch as a side effect of compress).
    cresp = client.post(
        "/api/compress", json={"use_generated": False, "logs": batch}
    )
    assert cresp.status_code == 200, cresp.text
    api_reduction = cresp.json()["delta_reduction"]

    # Directly via a store with NO analyzer, configured exactly like the app's store.
    bare_store = SegmentStore(
        keyframe_interval=100,
        baseline="previous",
        encoder_config=EncoderConfig.all_on(),
        gzip_deltas=False,
    )
    direct_reduction = bare_store.compress(batch).to_dict()["delta_reduction"]

    assert api_reduction == direct_reduction, (
        f"analyzer altered compression: API={api_reduction} vs direct={direct_reduction}"
    )

    # And the value the API reports in /api/stats matches too (same encode, same number).
    stats_reduction = client.get("/api/stats").json()["storage"]["delta_reduction"]
    assert stats_reduction == direct_reduction
