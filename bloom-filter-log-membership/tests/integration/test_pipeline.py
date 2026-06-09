"""Integration tests for the C10 two-tier pipeline: /pipeline/*.

Most tests ride the shared ``client`` fixture (real lifespan, tmp DATA_DIR
and SQLITE_PATH, spec-sized filters). The breach drills at the bottom build
their own environment instead: forcing a real FP-estimate breach against a
1M-capacity filter would take ~a million ingests, while a 100-capacity
filter with a loose 0.30 target crosses a 0.02 threshold within a couple
hundred distinct keys (sizing math at ``breach_env``).
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.settings import get_settings

PER_FILTER_STAT_KEYS = {
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

#: _totals carries the same keys minus the per-filter live flag.
TOTALS_STAT_KEYS = PER_FILTER_STAT_KEYS - {"fallback_active"}

LOOKUP_RESPONSE_KEYS = {
    "found",
    "might_exist",
    "source",
    "storage_checked",
    "false_positive",
    "fallback_active",
    "processing_time_ms",
}

INGEST_RESPONSE_KEYS = {"status", "bloom_updated", "duplicate", "processing_time_ms"}


def _ingest(client: TestClient, log_type: str, log_key: str) -> dict:
    response = client.post(
        "/pipeline/ingest", json={"log_type": log_type, "log_key": log_key}
    )
    assert response.status_code == 200
    return response.json()


def _lookup(client: TestClient, log_type: str, log_key: str) -> dict:
    response = client.post(
        "/pipeline/lookup", json={"log_type": log_type, "log_key": log_key}
    )
    assert response.status_code == 200
    return response.json()


# ---------------------------------------------------------------------- #
# two-tier basics                                                        #
# ---------------------------------------------------------------------- #


def test_ingest_then_lookup_found(client: TestClient) -> None:
    """Ingested key: both tiers updated, lookup verified against storage."""
    ingested = _ingest(client, "error_logs", "pipe-key-1")
    assert ingested["status"] == "stored"
    assert ingested["bloom_updated"] is True
    assert ingested["duplicate"] is False
    assert ingested["processing_time_ms"] >= 0.0

    answer = _lookup(client, "error_logs", "pipe-key-1")
    assert answer["found"] is True
    assert answer["might_exist"] is True
    assert answer["source"] == "storage"
    assert answer["storage_checked"] is True
    assert answer["false_positive"] is False
    assert answer["fallback_active"] is False


def test_absent_key_short_circuits_storage(client: TestClient) -> None:
    """THE Extended-C proof: a bloom negative skips the expensive tier."""
    _ingest(client, "error_logs", "present-key")

    answer = _lookup(client, "error_logs", "never-ingested-key")
    assert answer["found"] is False
    assert answer["might_exist"] is False
    assert answer["storage_checked"] is False
    assert answer["source"] == "bloom_negative"
    assert answer["false_positive"] is False
    assert answer["fallback_active"] is False

    stats = client.get("/pipeline/stats").json()["error_logs"]
    assert stats["bloom_negatives"] == 1
    assert stats["storage_skipped_pct"] > 0


def test_response_key_sets_are_exact(client: TestClient) -> None:
    """Both endpoints carry exactly the documented keys — nothing extra."""
    ingested = _ingest(client, "error_logs", "shape-key")
    assert set(ingested) == INGEST_RESPONSE_KEYS

    found = _lookup(client, "error_logs", "shape-key")
    absent = _lookup(client, "error_logs", "shape-absent")
    assert set(found) == LOOKUP_RESPONSE_KEYS
    assert set(absent) == LOOKUP_RESPONSE_KEYS


def test_duplicate_ingest_flags_duplicate_and_keeps_one_row(
    client: TestClient,
) -> None:
    """Re-ingest: status stays "stored" (the row exists), duplicate=True."""
    first = _ingest(client, "access_logs", "dup-key")
    second = _ingest(client, "access_logs", "dup-key")
    assert first["duplicate"] is False
    assert second["status"] == "stored"
    assert second["duplicate"] is True
    assert second["bloom_updated"] is True

    stats = client.get("/pipeline/stats").json()["access_logs"]
    assert stats["storage_rows"] == 1


def test_logs_add_only_key_is_a_pipeline_false_positive(
    client: TestClient,
) -> None:
    """/logs/add feeds the filter but NOT storage → the pipeline disproves it.

    From the pipeline's standpoint sqlite is ground truth, so a key living
    only in the filter is indistinguishable from a hash-collision false
    positive: bloom says probably, storage says no, and the disproof is
    counted in the pipeline tallies AND the filter's own metrics ledger.
    """
    response = client.post(
        "/logs/add", json={"log_type": "error_logs", "log_key": "ghost-key"}
    )
    assert response.status_code == 200

    answer = _lookup(client, "error_logs", "ghost-key")
    assert answer["might_exist"] is True  # the filter does claim it...
    assert answer["found"] is False  # ...but storage is the truth
    assert answer["storage_checked"] is True
    assert answer["source"] == "storage"
    assert answer["false_positive"] is True
    assert answer["fallback_active"] is False

    pipeline_stats = client.get("/pipeline/stats").json()["error_logs"]
    assert pipeline_stats["false_positives"] == 1
    assert pipeline_stats["observed_fp_rate"] > 0.0

    # The disproof surfaces in /stats too (FilterMetrics.record_false_positive).
    filter_stats = client.get("/stats").json()["filters"]["error_logs"]
    assert filter_stats["observed_false_positives"] >= 1


# ---------------------------------------------------------------------- #
# validation                                                             #
# ---------------------------------------------------------------------- #


def test_validation_rejects_unknown_type_and_empty_key(
    client: TestClient,
) -> None:
    for path in ("/pipeline/ingest", "/pipeline/lookup"):
        bad_type = client.post(
            path, json={"log_type": "weird_logs", "log_key": "k"}
        )
        assert bad_type.status_code == 422
        empty_key = client.post(
            path, json={"log_type": "error_logs", "log_key": ""}
        )
        assert empty_key.status_code == 422


# ---------------------------------------------------------------------- #
# /pipeline/stats                                                        #
# ---------------------------------------------------------------------- #


def test_stats_shape_and_totals(client: TestClient) -> None:
    """Every managed filter carries the full per-filter key set, plus _totals.

    The C11 ``sessions`` filter shows up here automatically — the pipeline
    counters are keyed by manager name, not by the ``log_type`` Literal.
    """
    _ingest(client, "error_logs", "stat-a")
    _ingest(client, "security_logs", "stat-b")
    _lookup(client, "error_logs", "stat-a")  # storage hit
    _lookup(client, "error_logs", "stat-miss")  # bloom negative

    stats = client.get("/pipeline/stats").json()
    assert set(stats) == {
        "error_logs",
        "access_logs",
        "security_logs",
        "sessions",
        "_totals",
    }
    for name in ("error_logs", "access_logs", "security_logs", "sessions"):
        assert set(stats[name]) == PER_FILTER_STAT_KEYS, name
    assert set(stats["_totals"]) == TOTALS_STAT_KEYS

    error_logs = stats["error_logs"]
    assert error_logs["storage_rows"] == 1
    assert error_logs["lookups"] == 2
    assert error_logs["bloom_negatives"] == 1
    assert error_logs["storage_skipped_pct"] == 50.0
    assert error_logs["storage_hits"] == 1
    assert error_logs["false_positives"] == 0
    assert error_logs["fallback_active"] is False
    assert error_logs["rotations_triggered"] == 0

    # Untouched filter stays zeroed; totals are the cross-filter sums.
    assert stats["access_logs"]["lookups"] == 0
    assert stats["access_logs"]["storage_skipped_pct"] == 0.0
    assert stats["_totals"]["storage_rows"] == 2
    assert stats["_totals"]["lookups"] == 2
    assert stats["_totals"]["bloom_negatives"] == 1
    assert stats["_totals"]["storage_skipped_pct"] == 50.0


# ---------------------------------------------------------------------- #
# breach drills — engineered FP-threshold breach on a tiny, loose filter #
# ---------------------------------------------------------------------- #

#: Distinct keys ingested per attempt before re-probing for the breach.
BREACH_BATCH = 200

#: Give up after this many keys. With breach_env's sizing the FIRST batch
#: should already breach (see the fixture docstring); if the SBF budget
#: formulas ever change, raise this — or loosen the env further (smaller
#: ERROR_LOGS_CAPACITY / larger ERROR_LOGS_FP_RATE / lower threshold).
BREACH_MAX_KEYS = 2000


@pytest.fixture
def breach_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Environment for a deterministic FP-estimate breach (own tmp dirs).

    Sizing math: with ERROR_LOGS_CAPACITY=100 and ERROR_LOGS_FP_RATE=0.30,
    slice 0 of the scalable filter gets the budget ``0.30 * (1 - 0.85) =
    0.045``. Once its 100 slots fill (after the first ~100 distinct
    ingests), its fill-based estimate reads ≈ 0.045 — which ALONE exceeds
    the 0.02 threshold set here, and later slices only push the compound
    estimate higher. So a couple hundred distinct ingests breach
    deterministically; the drive loop's 2000-key ceiling is pure headroom.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    monkeypatch.setenv("SQLITE_PATH", str(data_dir / "logs.db"))
    monkeypatch.setenv("SNAPSHOT_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("ROTATION_CHECK_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("ERROR_LOGS_CAPACITY", "100")
    monkeypatch.setenv("ERROR_LOGS_FP_RATE", "0.30")
    monkeypatch.setenv("FP_FALLBACK_THRESHOLD", "0.02")
    yield
    get_settings.cache_clear()


def _open_client(
    monkeypatch: pytest.MonkeyPatch, rotate_on_breach: bool
) -> TestClient:
    """Set FP_ROTATE_ON_BREACH, rebuild settings, and open the real app."""
    monkeypatch.setenv(
        "FP_ROTATE_ON_BREACH", "true" if rotate_on_breach else "false"
    )
    get_settings.cache_clear()
    from src.api import app

    return TestClient(app)


def _drive_into_breach(client: TestClient) -> dict:
    """Ingest distinct keys in batches until a probe reports fallback_active.

    Returns that first breached lookup response — with rotation enabled it
    is also the lookup that fired the rotation. Probes use fresh
    never-ingested keys so every iteration asks the same shaped question.
    Fails loudly with tuning advice if the estimate never crosses the
    threshold (see BREACH_MAX_KEYS).
    """
    ingested = 0
    while ingested < BREACH_MAX_KEYS:
        for i in range(ingested, ingested + BREACH_BATCH):
            _ingest(client, "error_logs", f"breach-key-{i:05d}")
        ingested += BREACH_BATCH
        probe = _lookup(client, "error_logs", f"breach-probe-{ingested}")
        if probe["fallback_active"]:
            return probe
    pytest.fail(
        f"FP estimate never breached the threshold after {ingested} distinct "
        "ingests; raise BREACH_MAX_KEYS or loosen the breach_env sizing "
        "(smaller ERROR_LOGS_CAPACITY / larger ERROR_LOGS_FP_RATE)."
    )


def test_breach_engages_fallback_and_storage_answers(
    breach_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rotation disabled: fallback persists and serves exact answers both ways."""
    with _open_client(monkeypatch, rotate_on_breach=False) as client:
        breached = _drive_into_breach(client)
        assert breached["fallback_active"] is True
        assert breached["storage_checked"] is True
        assert breached["source"] == "storage"
        assert breached["found"] is False  # the probe key was never ingested
        assert breached["false_positive"] is False  # bloom was not consulted

        # A PRESENT key under fallback: still found, straight from storage.
        present = _lookup(client, "error_logs", "breach-key-00000")
        assert present["fallback_active"] is True
        assert present["found"] is True
        assert present["might_exist"] is True
        assert present["source"] == "storage"
        assert present["storage_checked"] is True

        stats = client.get("/pipeline/stats").json()["error_logs"]
        assert stats["fallback_active"] is True  # live flag: still breached
        assert stats["fallback_lookups"] >= 2
        assert stats["rotations_triggered"] == 0


def test_breach_triggers_exactly_one_rotation_then_recovers(
    breach_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rotation enabled: one rotate per breach episode, then two-tier resumes."""
    with _open_client(monkeypatch, rotate_on_breach=True) as client:
        breached = _drive_into_breach(client)
        # The breach lookup itself is served from storage under fallback...
        assert breached["fallback_active"] is True
        assert breached["storage_checked"] is True
        assert breached["source"] == "storage"
        assert breached["found"] is False
        # ...and it fired the single rotation this breach episode gets.
        stats = client.get("/pipeline/stats").json()["error_logs"]
        assert stats["rotations_triggered"] == 1
        filter_stats = client.get("/stats").json()["filters"]["error_logs"]
        assert filter_stats["rotations"] == 1

        # The rotated-in current generation reads ≈0 estimate, so fallback
        # deactivates and further lookups are two-tier again — the rotation
        # counter must NOT move (one rotation per breach episode).
        last: dict = {}
        for i in range(3):
            last = _lookup(client, "error_logs", f"post-rotation-absent-{i}")
        assert last["fallback_active"] is False
        stats = client.get("/pipeline/stats").json()["error_logs"]
        assert stats["rotations_triggered"] == 1
        assert stats["fallback_active"] is False

        # An ingested key is still found after rotation: the demoted previous
        # generation answers the bloom side, and sqlite stays ground truth.
        present = _lookup(client, "error_logs", "breach-key-00000")
        assert present["found"] is True
        assert present["fallback_active"] is False
        assert present["source"] == "storage"
        assert present["storage_checked"] is True
