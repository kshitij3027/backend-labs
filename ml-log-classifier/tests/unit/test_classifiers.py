"""Unit tests for :mod:`src.classifiers` (Commit 5).

These exercise the three base-classifier factories
(:func:`make_naive_bayes`, :func:`make_random_forest`,
:func:`make_gradient_boosting`), the ordered :func:`build_base_classifiers`
mapping, and the thin scoring helpers (:func:`predict_with_confidence`,
:func:`evaluate`).

Two flavours of test:

* **Parameter assertions** — purely construct the (unfitted) estimators and
  inspect ``get_params()``. These verify the sklearn-API gotchas the impl agent
  called out: ``MultinomialNB`` must have *no* ``class_weight`` / ``random_state``;
  ``GradientBoostingClassifier`` must have *no* ``class_weight``; only the random
  forest gets ``class_weight="balanced"``. They need no fitting and are instant.
* **Functional smoke tests** — fit each estimator on a tiny, real, non-negative
  CSR matrix from :class:`src.features.FeaturePipeline` and confirm
  ``predict`` / ``predict_proba`` shapes plus the helper contracts. To keep these
  fast, GB/RF use a custom ``Settings`` with a small ``n_estimators`` and only
  ≤60 generated records.
"""

from __future__ import annotations

import numpy as np
import pytest
from scipy.sparse import csr_matrix
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.naive_bayes import MultinomialNB

from src.classifiers import (
    CLASSIFIER_NAMES,
    build_base_classifiers,
    evaluate,
    make_gradient_boosting,
    make_naive_bayes,
    make_random_forest,
    predict_with_confidence,
)
from src.config import Settings
from src.features import FeaturePipeline
from src.log_generator import generate_logs


# ---------------------------------------------------------------------------
# Fixtures — a tiny, deterministic, real feature matrix shared across the
# functional tests so the (cheap) feature fit happens once.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def small_fast_cfg() -> Settings:
    """A config with tiny tree counts so GB/RF fit quickly in the smoke tests."""
    return Settings(rf_n_estimators=10, gb_n_estimators=10)


@pytest.fixture(scope="module")
def small_matrix():
    """Return ``(X, y)``: a small non-negative CSR matrix + severity labels.

    Built from the real :class:`FeaturePipeline` on 60 deterministic records so
    the smoke tests exercise the actual sparse, non-negative input the
    classifiers must accept.
    """
    recs = generate_logs(60, 42)
    X = FeaturePipeline().fit_transform(recs)
    y = [r["severity"] for r in recs]
    return X, y


# ---------------------------------------------------------------------------
# Parameter assertions (no fitting).
# ---------------------------------------------------------------------------


def test_random_forest_default_params():
    """RF factory wires the spec'd hyperparameters from default config."""
    params = make_random_forest().get_params()
    assert params["class_weight"] == "balanced"
    assert params["n_estimators"] == 100
    assert params["random_state"] == 42
    assert params["n_jobs"] == -1


def test_gradient_boosting_default_params():
    """GB factory sets n_estimators/random_state and never gets class_weight."""
    params = make_gradient_boosting().get_params()
    assert params["n_estimators"] == 100
    assert params["random_state"] == 42
    # GradientBoostingClassifier has no class_weight parameter at all — passing
    # one would raise; assert the API surface to lock that expectation in.
    assert "class_weight" not in GradientBoostingClassifier().get_params()


def test_naive_bayes_type_and_unsupported_params():
    """NB factory returns a MultinomialNB with no class_weight/random_state."""
    nb = make_naive_bayes()
    assert isinstance(nb, MultinomialNB)
    params = nb.get_params()
    assert "class_weight" not in params
    assert "random_state" not in params


def test_custom_cfg_threads_through_factories():
    """A custom Settings is reflected by every factory's hyperparameters."""
    cfg = Settings(rf_n_estimators=7, gb_n_estimators=5, random_seed=123)

    rf = make_random_forest(cfg).get_params()
    assert rf["n_estimators"] == 7
    assert rf["random_state"] == 123
    assert rf["class_weight"] == "balanced"

    gb = make_gradient_boosting(cfg).get_params()
    assert gb["n_estimators"] == 5
    assert gb["random_state"] == 123

    # NB has no tunable params here but must still construct cleanly with a cfg.
    assert isinstance(make_naive_bayes(cfg), MultinomialNB)


def test_build_base_classifiers_keys_and_types():
    """build_base_classifiers yields the ordered nb/rf/gb mapping with right types."""
    clfs = build_base_classifiers()
    assert list(clfs.keys()) == ["nb", "rf", "gb"]
    assert isinstance(clfs["nb"], MultinomialNB)
    assert isinstance(clfs["rf"], RandomForestClassifier)
    assert isinstance(clfs["gb"], GradientBoostingClassifier)
    # The public name tuple is the contract the ensemble/weights rely on.
    assert CLASSIFIER_NAMES == ("nb", "rf", "gb")


def test_build_base_classifiers_threads_custom_cfg():
    """A cfg passed to build_base_classifiers reaches each estimator."""
    cfg = Settings(rf_n_estimators=7, gb_n_estimators=5, random_seed=123)
    clfs = build_base_classifiers(cfg)
    assert clfs["rf"].get_params()["n_estimators"] == 7
    assert clfs["rf"].get_params()["random_state"] == 123
    assert clfs["gb"].get_params()["n_estimators"] == 5
    assert clfs["gb"].get_params()["random_state"] == 123


# ---------------------------------------------------------------------------
# Functional smoke tests (fit on a tiny real matrix; kept fast).
# ---------------------------------------------------------------------------


def test_each_classifier_fits_predicts_and_exposes_proba(small_matrix, small_fast_cfg):
    """Each of nb/rf/gb fits the real CSR matrix and yields correct shapes."""
    X, y = small_matrix
    n_classes = len(set(y))
    for name, est in build_base_classifiers(small_fast_cfg).items():
        est.fit(X, y)
        preds = est.predict(X)
        assert len(preds) == X.shape[0], f"{name}: bad prediction count"
        proba = est.predict_proba(X)
        assert proba.shape == (X.shape[0], n_classes), f"{name}: bad proba shape"


def test_predict_with_confidence_contract(small_matrix, small_fast_cfg):
    """predict_with_confidence returns aligned labels + [0,1] confidences."""
    X, y = small_matrix
    valid = set(y)
    est = make_random_forest(small_fast_cfg).fit(X, y)

    labels, confidences = predict_with_confidence(est, X)
    assert len(labels) == len(confidences) == X.shape[0]
    assert all(0.0 <= c <= 1.0 for c in confidences)
    assert all(isinstance(c, float) for c in confidences)
    assert all(lbl in valid for lbl in labels)


def test_evaluate_returns_float_in_unit_interval(small_matrix, small_fast_cfg):
    """evaluate returns a plain float accuracy within [0, 1]."""
    X, y = small_matrix
    est = make_naive_bayes(small_fast_cfg).fit(X, y)
    acc = evaluate(est, X, y)
    assert isinstance(acc, float)
    assert 0.0 <= acc <= 1.0


def test_multinomial_nb_accepts_handmade_nonnegative_sparse():
    """MultinomialNB fits/predicts a tiny hand-made non-negative CSR with 2 classes."""
    # 4 samples x 3 features, all non-negative (the MultinomialNB requirement).
    X = csr_matrix(
        np.array(
            [
                [2.0, 0.0, 1.0],
                [3.0, 1.0, 0.0],
                [0.0, 2.0, 3.0],
                [0.0, 3.0, 2.0],
            ]
        )
    )
    y = ["a", "a", "b", "b"]
    nb = make_naive_bayes().fit(X, y)
    preds = nb.predict(X)
    assert len(preds) == X.shape[0]
    assert set(preds).issubset({"a", "b"})
    assert nb.predict_proba(X).shape == (X.shape[0], 2)
