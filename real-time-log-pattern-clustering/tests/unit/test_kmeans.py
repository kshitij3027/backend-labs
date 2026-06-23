"""Unit tests for the K-means clusterer (:mod:`src.clustering.kmeans`).

These pin the streaming :class:`~src.clustering.base.Clusterer` contract that the engine
(C8) and the sibling density clusterers (C6/C7) rely on, exercised through the K-means
implementation:

* :meth:`warm_fit` then :meth:`assign` returns a :class:`ClusterResult` whose ``labels`` /
  ``confidences`` / ``anomalies`` are parallel length-``n`` arrays, with confidences in
  ``[0, 1]`` and labels valid K-means cluster ids (``0..k-1``, never ``-1``),
* a point far outside every blob is flagged as an anomaly while a point at a blob centre is
  not (the distance-from-centroid rule),
* :meth:`assign` accepts a single 1-D vector,
* :meth:`n_clusters` / :meth:`cluster_sizes` reflect the fit (sizes sum to the training n),
* determinism: same data + ``random_state`` => identical labels across two warm-fits,
* :meth:`refit` updates without error and keeps the model fitted, and
* :meth:`assign` before any fit raises a clear error.

``sklearn.datasets.make_blobs`` provides clean, well-separated clusterable data so the
assertions about labels / anomalies are deterministic and don't depend on the real feature
pipeline.
"""

from __future__ import annotations

import numpy as np
import pytest
from sklearn.datasets import make_blobs

from src.clustering.base import Clusterer, ClusterResult
from src.clustering.kmeans import KMeansClusterer
from src.config import AppConfig

# --------------------------------------------------------------------------- #
# Fixtures / constants
# --------------------------------------------------------------------------- #

_N_SAMPLES = 300
_N_CENTERS = 5
_N_FEATURES = 20


def _blobs(random_state: int = 0):
    """Return a clean ``(X, y)`` blob dataset for clustering (5 well-separated centres)."""
    X, y = make_blobs(
        n_samples=_N_SAMPLES,
        centers=_N_CENTERS,
        n_features=_N_FEATURES,
        random_state=random_state,
    )
    return X.astype(np.float64), y


@pytest.fixture
def fitted_clusterer() -> KMeansClusterer:
    """A :class:`KMeansClusterer` (k=5) warm-fit on the canonical blob dataset."""
    cfg = AppConfig()
    cfg.kmeans.n_clusters = _N_CENTERS
    clusterer = KMeansClusterer(cfg)
    X, _ = _blobs()
    clusterer.warm_fit(X)
    return clusterer


# --------------------------------------------------------------------------- #
# Contract / type checks
# --------------------------------------------------------------------------- #


def test_is_a_clusterer_with_name() -> None:
    """KMeansClusterer is a Clusterer subclass tagged ``"kmeans"``."""
    clusterer = KMeansClusterer(AppConfig())
    assert isinstance(clusterer, Clusterer)
    assert clusterer.name == "kmeans"
    assert clusterer.is_fitted is False


def test_assign_returns_parallel_cluster_result(fitted_clusterer: KMeansClusterer) -> None:
    """assign(X) returns a ClusterResult with three parallel length-n arrays."""
    X, _ = _blobs()
    result = fitted_clusterer.assign(X)

    assert isinstance(result, ClusterResult)
    assert result.labels.shape == (_N_SAMPLES,)
    assert result.confidences.shape == (_N_SAMPLES,)
    assert result.anomalies.shape == (_N_SAMPLES,)

    # dtypes per the contract: int labels, float confidences, bool anomalies.
    assert np.issubdtype(result.labels.dtype, np.integer)
    assert np.issubdtype(result.confidences.dtype, np.floating)
    assert result.anomalies.dtype == np.bool_


def test_confidences_within_unit_interval(fitted_clusterer: KMeansClusterer) -> None:
    """Confidences are finite and lie within [0, 1] (never NaN/Inf)."""
    X, _ = _blobs()
    conf = fitted_clusterer.assign(X).confidences
    assert np.all(np.isfinite(conf))
    assert np.all(conf >= 0.0)
    assert np.all(conf <= 1.0)


def test_labels_are_valid_kmeans_ids_never_noise(
    fitted_clusterer: KMeansClusterer,
) -> None:
    """K-means assigns real clusters in 0..k-1; it never emits -1."""
    X, _ = _blobs()
    labels = fitted_clusterer.assign(X).labels
    k = fitted_clusterer.model.n_clusters
    assert labels.min() >= 0
    assert labels.max() < k
    assert not np.any(labels == -1)


def test_in_cluster_point_has_high_confidence(
    fitted_clusterer: KMeansClusterer,
) -> None:
    """A point sitting on a real cluster centroid is assigned with strong confidence."""
    centroid = fitted_clusterer.model.cluster_centers_[0]
    result = fitted_clusterer.assign(centroid)
    assert result.confidences[0] > 0.5


# --------------------------------------------------------------------------- #
# Anomaly behaviour (distance-from-centroid rule)
# --------------------------------------------------------------------------- #


def test_far_point_flagged_anomaly_center_not(
    fitted_clusterer: KMeansClusterer,
) -> None:
    """A point far outside all blobs is an anomaly; a blob-centre point is not."""
    far = np.full(_N_FEATURES, 50.0)
    far_result = fitted_clusterer.assign(far)
    assert bool(far_result.anomalies[0]) is True

    centroid = fitted_clusterer.model.cluster_centers_[0]
    center_result = fitted_clusterer.assign(centroid)
    assert bool(center_result.anomalies[0]) is False


def test_bulk_inliers_mostly_not_anomalous(
    fitted_clusterer: KMeansClusterer,
) -> None:
    """The vast majority of the training-like blobs are NOT flagged (low false-positive)."""
    X, _ = _blobs()
    anomalies = fitted_clusterer.assign(X).anomalies
    # ~3-sigma threshold => only a small tail should trip.
    assert anomalies.mean() < 0.05


# --------------------------------------------------------------------------- #
# Single-vector / shape handling
# --------------------------------------------------------------------------- #


def test_assign_accepts_single_1d_vector(fitted_clusterer: KMeansClusterer) -> None:
    """assign accepts a single 1-D vector (shape (d,)) -> length-1 result arrays."""
    vec = np.zeros(_N_FEATURES)
    result = fitted_clusterer.assign(vec)
    assert result.labels.shape == (1,)
    assert result.confidences.shape == (1,)
    assert result.anomalies.shape == (1,)


def test_assign_single_row_2d(fitted_clusterer: KMeansClusterer) -> None:
    """assign also accepts an explicit (1, d) row."""
    row = np.zeros((1, _N_FEATURES))
    result = fitted_clusterer.assign(row)
    assert result.labels.shape == (1,)


# --------------------------------------------------------------------------- #
# Cluster statistics helpers (shared base implementation)
# --------------------------------------------------------------------------- #


def test_n_clusters_matches_configuration(fitted_clusterer: KMeansClusterer) -> None:
    """n_clusters() reports the configured/fitted cluster count (no noise label)."""
    assert fitted_clusterer.n_clusters() == _N_CENTERS


def test_cluster_sizes_sum_to_training_n(fitted_clusterer: KMeansClusterer) -> None:
    """cluster_sizes() maps id->count and the counts sum to the training row count."""
    sizes = fitted_clusterer.cluster_sizes()
    assert isinstance(sizes, dict)
    assert sum(sizes.values()) == _N_SAMPLES
    # K-means training labels carry no noise bucket.
    assert -1 not in sizes


def test_stats_empty_before_fit() -> None:
    """Before any fit the statistics helpers are empty/zero rather than raising."""
    clusterer = KMeansClusterer(AppConfig())
    assert clusterer.n_clusters() == 0
    assert clusterer.cluster_sizes() == {}


# --------------------------------------------------------------------------- #
# Determinism & refit
# --------------------------------------------------------------------------- #


def test_warm_fit_is_deterministic() -> None:
    """Same data + random_state => identical labels across two independent warm-fits."""
    cfg = AppConfig()
    cfg.kmeans.n_clusters = _N_CENTERS
    X, _ = _blobs()

    a = KMeansClusterer(cfg)
    a.warm_fit(X)
    b = KMeansClusterer(cfg)
    b.warm_fit(X)

    np.testing.assert_array_equal(a.assign(X).labels, b.assign(X).labels)


def test_refit_updates_and_stays_fitted(fitted_clusterer: KMeansClusterer) -> None:
    """refit on a fresh blob set runs without error and keeps the model fitted."""
    X2, _ = _blobs(random_state=7)
    fitted_clusterer.refit(X2)
    assert fitted_clusterer.is_fitted is True

    result = fitted_clusterer.assign(X2)
    assert result.labels.shape == (_N_SAMPLES,)
    assert np.all(np.isfinite(result.confidences))


# --------------------------------------------------------------------------- #
# Error handling
# --------------------------------------------------------------------------- #


def test_assign_before_fit_raises() -> None:
    """Calling assign before warm_fit/refit raises a clear RuntimeError."""
    clusterer = KMeansClusterer(AppConfig())
    X, _ = _blobs()
    with pytest.raises(RuntimeError, match="before fit"):
        clusterer.assign(X)


def test_warm_fit_empty_raises() -> None:
    """Fitting on an empty matrix raises ValueError."""
    clusterer = KMeansClusterer(AppConfig())
    with pytest.raises(ValueError):
        clusterer.warm_fit(np.empty((0, _N_FEATURES)))
