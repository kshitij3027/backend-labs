"""Unit tests for :mod:`src.ensemble` (Commit 6).

Covers the two public surfaces of the ensemble layer:

* :func:`build_ensemble` — the unfitted soft-voting ``VotingClassifier`` over the
  ``nb`` / ``rf`` / ``gb`` base estimators with weights ``[1, 2, 3]``.
* :class:`LogClassifier` — the raw-text → ``{severity, category, confidence, ...}``
  façade: single + batch classification, the aggregate-confidence contract, the
  save/load round-trip, and the before-fit guard.

Most assertions need a *fitted* classifier. Training a real (NB + RF + GB) dual
ensemble is the slow part, so a single ``trained_clf`` is fit **once** per module
(on 800 deterministic records — quick but representative) and shared across the
functional tests. The canonical success criterion (project requirements §5)
is asserted explicitly: ``"Database connection failed with timeout error"`` must
classify as ``severity == "ERROR"`` and ``category == "SYSTEM"``.
"""

from __future__ import annotations

import pytest
from sklearn.ensemble import VotingClassifier

from src.ensemble import LogClassifier, build_ensemble
from src.log_generator import CATEGORIES, SEVERITIES, generate_logs

#: The canonical example from the spec's success criterion (§5):
#: input → Severity: ERROR, Category: SYSTEM.
CANONICAL_INPUT = "Database connection failed with timeout error"
CANONICAL_SEVERITY = "ERROR"
CANONICAL_CATEGORY = "SYSTEM"

#: Exact key set every classification result dict must expose.
EXPECTED_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}


# ---------------------------------------------------------------------------
# Module-scoped fixture — train the dual ensemble exactly once.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def trained_clf() -> LogClassifier:
    """A :class:`LogClassifier` fitted once on 800 deterministic records.

    800 records keep the full NB + RF + GB dual-ensemble fit quick while staying
    representative enough that the canonical example classifies correctly. Shared
    across every functional test in this module (module scope) so the expensive
    fit happens a single time.
    """
    return LogClassifier().fit(generate_logs(800, 42))


def _assert_result_shape(out: dict) -> None:
    """Assert ``out`` has exactly the 5 keys with in-range, correctly-typed values."""
    assert set(out.keys()) == EXPECTED_KEYS, f"unexpected keys: {sorted(out.keys())}"
    assert out["severity"] in SEVERITIES, f"bad severity: {out['severity']!r}"
    assert out["category"] in CATEGORIES, f"bad category: {out['category']!r}"
    for key in ("confidence", "severity_confidence", "category_confidence"):
        val = out[key]
        assert isinstance(val, float), f"{key} should be a native float, got {type(val)}"
        assert 0.0 <= val <= 1.0, f"{key} out of [0, 1]: {val}"


# ---------------------------------------------------------------------------
# build_ensemble — construction contract (no fitting required).
# ---------------------------------------------------------------------------


def test_build_ensemble_construction_contract():
    """build_ensemble() is an unfitted soft VotingClassifier with the right wiring."""
    ens = build_ensemble()
    assert isinstance(ens, VotingClassifier)
    assert ens.voting == "soft"
    assert list(ens.weights) == [1, 2, 3]
    # Estimator order/names are the contract ensemble_weights aligns to.
    names = [name for name, _ in ens.estimators]
    assert names == ["nb", "rf", "gb"]
    # Unfitted: a fitted VotingClassifier exposes ``estimators_`` (trailing _).
    assert not hasattr(ens, "estimators_")


# ---------------------------------------------------------------------------
# LogClassifier — single classify().
# ---------------------------------------------------------------------------


def test_classify_returns_exact_keys_and_ranges(trained_clf):
    """classify() yields exactly the 5 keys with valid labels + [0,1] confidences."""
    out = trained_clf.classify(CANONICAL_INPUT)
    _assert_result_shape(out)


def test_classify_canonical_success_criterion(trained_clf):
    """Canonical spec example → severity ERROR and category SYSTEM (§5).

    This is the headline success criterion. On failure, surface the full result
    dict so the actual labels/confidences are visible for a generator/feature tweak.
    """
    out = trained_clf.classify(CANONICAL_INPUT)
    assert out["severity"] == CANONICAL_SEVERITY, f"canonical severity mismatch: {out}"
    assert out["category"] == CANONICAL_CATEGORY, f"canonical category mismatch: {out}"


def test_confidence_is_mean_of_axis_confidences(trained_clf):
    """confidence == round((severity_confidence + category_confidence) / 2, 4)."""
    out = trained_clf.classify(CANONICAL_INPUT)
    expected = round((out["severity_confidence"] + out["category_confidence"]) / 2, 4)
    assert out["confidence"] == expected, f"aggregate confidence inconsistent: {out}"


# ---------------------------------------------------------------------------
# LogClassifier — classify_batch().
# ---------------------------------------------------------------------------


def test_classify_batch_accepts_mixed_str_and_dict(trained_clf):
    """classify_batch() handles a mixed [str, dict] batch and returns 2 result dicts."""
    results = trained_clf.classify_batch(
        [CANONICAL_INPUT, {"raw_log": "GET /api/users 200 ok"}]
    )
    assert isinstance(results, list)
    assert len(results) == 2
    for out in results:
        _assert_result_shape(out)


def test_batch_matches_single_for_same_input(trained_clf):
    """A single-element batch agrees with classify() on severity for several inputs."""
    for text in (CANONICAL_INPUT, "GET /api/users 200 ok", "User login successful"):
        batch_sev = trained_clf.classify_batch([text])[0]["severity"]
        single_sev = trained_clf.classify(text)["severity"]
        assert batch_sev == single_sev, f"batch/single mismatch for {text!r}"


# ---------------------------------------------------------------------------
# LogClassifier — persistence round-trip.
# ---------------------------------------------------------------------------


def test_save_load_round_trip_preserves_predictions(trained_clf, tmp_path):
    """Reloaded classifier yields the SAME severity/category for the canonical input."""
    save_dir = tmp_path / "model"
    trained_clf.save(str(save_dir))

    # The four documented artifacts must land on disk.
    for fname in (
        "feature_pipeline.joblib",
        "severity_ensemble.joblib",
        "category_ensemble.joblib",
        "meta.json",
    ):
        assert (save_dir / fname).is_file(), f"missing artifact: {fname}"

    reloaded = LogClassifier.load(str(save_dir))
    assert reloaded.is_fitted

    before = trained_clf.classify(CANONICAL_INPUT)
    after = reloaded.classify(CANONICAL_INPUT)
    assert after["severity"] == before["severity"]
    assert after["category"] == before["category"]


# ---------------------------------------------------------------------------
# LogClassifier — guards & smoke.
# ---------------------------------------------------------------------------


def test_classify_before_fit_raises_runtime_error():
    """Calling classify() before fit()/load() raises a clear RuntimeError."""
    with pytest.raises(RuntimeError):
        LogClassifier().classify("x")


def test_classify_batch_before_fit_raises_runtime_error():
    """classify_batch() is likewise guarded before the classifier is fitted."""
    with pytest.raises(RuntimeError):
        LogClassifier().classify_batch(["x"])


def test_web_request_line_classifies_to_valid_severity(trained_clf):
    """A clearly INFO-ish web request line classifies to some valid severity (smoke)."""
    out = trained_clf.classify("Request handled GET /api/v1/users status=200 took 12ms")
    _assert_result_shape(out)
    assert out["severity"] in SEVERITIES
