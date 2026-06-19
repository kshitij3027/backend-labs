"""Unit tests for :mod:`src.trainer` (Commit 7).

Covers the training orchestration surface:

* :func:`src.trainer.train` — the full split → fit → cross-validate → final-fit
  flow: the exact 11-key metrics dict, accuracy/CV values in ``[0, 1]``, the
  ``n_train + n_test == n_total`` split invariant, the ``persist`` toggle
  (``version is None`` vs. a real ``"v1"`` written into a registry), and that the
  returned final classifier actually classifies.
* :func:`src.trainer.cross_validate` — the CV-only entry point: the four CV keys
  + fold count, means in ``[0, 1]``, and fold-count clamping for an absurd ``cv``.
* The stratify fallback path on a degenerate corpus (must not raise).

Everything runs with **tiny** estimators (10-tree RF + 10-stage GB), ``cv=3`` and
at most 120 deterministic records so the real dual-ensemble fits stay fast.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.ensemble import LogClassifier
from src.log_generator import generate_logs
from src.model_store import ModelRegistry
from src.trainer import cross_validate, train

#: The 11 keys :func:`train` must put in its ``metrics`` dict.
EXPECTED_METRIC_KEYS = {
    "severity_test_accuracy",
    "category_test_accuracy",
    "severity_cv_mean",
    "severity_cv_std",
    "category_cv_mean",
    "category_cv_std",
    "n_total",
    "n_train",
    "n_test",
    "cv",
    "trained_at",
}

#: Subset of metric keys that must be floats inside ``[0, 1]``.
UNIT_INTERVAL_KEYS = (
    "severity_test_accuracy",
    "category_test_accuracy",
    "severity_cv_mean",
    "category_cv_mean",
)

#: Canonical line used to confirm the returned classifier can classify.
CANONICAL_INPUT = "Database connection failed with timeout error"


@pytest.fixture(scope="module")
def tiny_cfg() -> Settings:
    """Fast config: 10-tree RF + 10-stage GB. Shared (read-only) across the module."""
    return Settings(rf_n_estimators=10, gb_n_estimators=10)


def test_train_no_persist_metrics_and_classifier(tiny_cfg) -> None:
    """``train(persist=False)`` returns the full metrics dict, no version, a live clf."""
    # NB: for small counts the generator's per-class floors dominate, so the
    # corpus is larger than the requested 120 — assert against the real length.
    records = generate_logs(120, 42)
    result = train(records=records, cfg=tiny_cfg, cv=3, persist=False)

    metrics = result["metrics"]
    assert set(metrics.keys()) == EXPECTED_METRIC_KEYS, (
        f"unexpected metric keys: {sorted(metrics)}"
    )

    # Accuracy / CV means are floats in [0, 1].
    for key in UNIT_INTERVAL_KEYS:
        val = metrics[key]
        assert isinstance(val, float), f"{key} should be float, got {type(val)}"
        assert 0.0 <= val <= 1.0, f"{key} out of [0, 1]: {val}"

    # Split invariant + total record count (== the real generated corpus size).
    assert metrics["n_total"] == len(records)
    assert metrics["n_train"] + metrics["n_test"] == metrics["n_total"]
    assert metrics["cv"] == 3

    # persist=False => no version persisted.
    assert result["version"] is None

    # The returned final model is fitted and classifies into a 5-key dict.
    clf = result["classifier"]
    assert isinstance(clf, LogClassifier)
    out = clf.classify(CANONICAL_INPUT)
    assert {"severity", "category", "confidence"} <= set(out.keys())


def test_train_persist_writes_version_into_registry(tmp_path, tiny_cfg) -> None:
    """``train(persist=True, registry=...)`` mints ``v1`` and populates the registry."""
    registry = ModelRegistry(str(tmp_path))
    result = train(
        records=generate_logs(120, 42),
        cfg=tiny_cfg,
        cv=3,
        persist=True,
        registry=registry,
    )

    assert result["version"] == "v1"
    assert isinstance(result["version"], str)
    assert result["registry"] is registry
    assert registry.has_models() is True
    assert registry.current_version == "v1"
    # The stored entry carries the headline accuracy from the metrics dict.
    assert registry.list_versions()[0]["accuracy"] == pytest.approx(
        result["metrics"]["severity_test_accuracy"]
    )


def test_cross_validate_returns_cv_keys(tiny_cfg) -> None:
    """``cross_validate`` returns the 4 CV stats + fold count, means in [0, 1]."""
    metrics = cross_validate(generate_logs(120, 42), tiny_cfg, cv=3)

    assert set(metrics.keys()) == {
        "severity_cv_mean",
        "severity_cv_std",
        "category_cv_mean",
        "category_cv_std",
        "cv",
    }
    assert metrics["cv"] == 3
    for key in ("severity_cv_mean", "category_cv_mean"):
        val = metrics[key]
        assert isinstance(val, float)
        assert 0.0 <= val <= 1.0, f"{key} out of [0, 1]: {val}"


def test_cross_validate_clamps_absurd_fold_count(tiny_cfg) -> None:
    """An over-large ``cv`` is silently clamped to ``n`` rather than crashing."""
    records = generate_logs(120, 42)
    metrics = cross_validate(records, tiny_cfg, cv=999)
    # Folds clamped into [2, n_records]; must not exceed the corpus size.
    assert 2 <= metrics["cv"] <= len(records)
    assert 0.0 <= metrics["severity_cv_mean"] <= 1.0


def test_train_degenerate_corpus_does_not_raise(tiny_cfg) -> None:
    """An imbalanced corpus with a singleton class trains without raising.

    One severity/category appears exactly once, so stratification on severity is
    infeasible (``_can_stratify`` needs >= 2 members per class). The trainer must
    transparently fall back to a plain random split — and the CV fold count must
    clamp to the rarest class (1 -> 2) — rather than letting sklearn raise. Both
    axes still carry >= 2 classes so the estimators themselves can fit.
    """
    records = [
        {
            "raw_log": f"Worker process crashed unexpectedly pid={i}",
            "service": "web",
            "severity": "CRITICAL",
            "category": "SYSTEM",
            "timestamp": "2026-06-21T00:00:00",
        }
        for i in range(11)
    ]
    # A single outlier record of a different severity + category -> singleton
    # classes that make a stratified split impossible.
    records.append(
        {
            "raw_log": "User login successful user_id=7 from 10.0.0.1",
            "service": "web",
            "severity": "INFO",
            "category": "AUTH",
            "timestamp": "2026-06-21T00:00:00",
        }
    )
    result = train(records=records, cfg=tiny_cfg, cv=3, persist=False)
    assert set(result["metrics"].keys()) == EXPECTED_METRIC_KEYS
    assert result["metrics"]["n_total"] == 12
    assert result["metrics"]["n_train"] + result["metrics"]["n_test"] == 12
