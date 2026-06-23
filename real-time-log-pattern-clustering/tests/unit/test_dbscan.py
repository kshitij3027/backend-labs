"""Unit tests for the DBSCAN clusterer (:mod:`src.clustering.dbscan`).

These pin the streaming :class:`~src.clustering.base.Clusterer` contract — the same one the
engine (C8) and the sibling clusterers (K-means C5 / HDBSCAN C7) share — exercised through
the DBSCAN implementation and its nearest-core streaming approximation:

* :meth:`warm_fit` finds the dense blobs (``n_clusters() >= 2``), and :meth:`assign` returns a
  :class:`ClusterResult` whose ``labels`` / ``confidences`` / ``anomalies`` are parallel
  length-``n`` arrays with confidences in ``[0, 1]``,
* a point at a blob centre is assigned a real (non-negative) cluster with positive confidence,
  while a point far from every blob is ``-1`` and flagged as an anomaly (the density rule),
* :meth:`assign` accepts a single 1-D vector,
* :meth:`n_clusters` (excludes the ``-1`` bucket) / :meth:`cluster_sizes` (includes counts)
  reflect the fit,
* :meth:`refit` on fresh blobs keeps it working and fitted,
* :meth:`assign` before any fit raises, and
* an all-noise fit (tiny ``eps``, zero core points) makes :meth:`assign` return all ``-1`` /
  anomalies without crashing.

``sklearn.datasets.make_blobs`` provides clean, well-separated clusterable data; ``eps`` is
overridden to a blob-appropriate scale (the config default 0.3 is tuned for normalized
features, far too small for raw blob coordinates).
"""

from __future__ import annotations

import numpy as np
import pytest
from sklearn.datasets import make_blobs

from src.clustering.base import Clusterer, ClusterResult
from src.clustering.dbscan import DBSCANClusterer
from src.config import AppConfig, load_config

# --------------------------------------------------------------------------- #
# Fixtures / constants
# --------------------------------------------------------------------------- #

_N_SAMPLES = 300
_N_CENTERS = 4
_N_FEATURES = 10
_CLUSTER_STD = 0.6

#: eps tuned to the blob scale (default 0.3 suits normalized vectors, not raw blobs).
_EPS = 1.5
_MIN_SAMPLES = 5


def _blobs(random_state: int = 0):
    """Return a clean ``(X, y)`` blob dataset (4 well-separated, tight centres)."""
    X, y = make_blobs(
        n_samples=_N_SAMPLES,
        centers=_N_CENTERS,
        n_features=_N_FEATURES,
        cluster_std=_CLUSTER_STD,
        random_state=random_state,
    )
    return X.astype(np.float64), y


def _blob_config(eps: float = _EPS, min_samples: int = _MIN_SAMPLES) -> AppConfig:
    """Load the default config and override DBSCAN eps/min_samples for the blob scale."""
    cfg = load_config()
    cfg.dbscan.eps = eps
    cfg.dbscan.min_samples = min_samples
    return cfg


@pytest.fixture
def fitted_clusterer() -> DBSCANClusterer:
    """A :class:`DBSCANClusterer` warm-fit on the canonical blob dataset (blob-scale eps)."""
    clusterer = DBSCANClusterer(_blob_config())
    X, _ = _blobs()
    clusterer.warm_fit(X)
    return clusterer


# --------------------------------------------------------------------------- #
# Contract / type checks
# --------------------------------------------------------------------------- #


def test_is_a_clusterer_with_name() -> None:
    """DBSCANClusterer is a Clusterer subclass tagged ``"dbscan"``."""
    clusterer = DBSCANClusterer(_blob_config())
    assert isinstance(clusterer, Clusterer)
    assert clusterer.name == "dbscan"
    assert clusterer.is_fitted is False


def test_warm_fit_finds_multiple_clusters(fitted_clusterer: DBSCANClusterer) -> None:
    """warm_fit on well-separated blobs discovers at least two non-noise clusters."""
    assert fitted_clusterer.is_fitted is True
    assert fitted_clusterer.n_clusters() >= 2


def test_assign_returns_parallel_cluster_result(
    fitted_clusterer: DBSCANClusterer,
) -> None:
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


def test_confidences_within_unit_interval(fitted_clusterer: DBSCANClusterer) -> None:
    """Confidences are finite and lie within [0, 1] (never NaN/Inf)."""
    X, _ = _blobs()
    conf = fitted_clusterer.assign(X).confidences
    assert np.all(np.isfinite(conf))
    assert np.all(conf >= 0.0)
    assert np.all(conf <= 1.0)


# --------------------------------------------------------------------------- #
# Density / nearest-core assignment behaviour
# --------------------------------------------------------------------------- #


def test_center_point_assigned_real_cluster_with_confidence(
    fitted_clusterer: DBSCANClusterer,
) -> None:
    """A point at a blob centre joins a real (non-negative) cluster with confidence > 0."""
    X, y = _blobs()
    # Centroid of the first blob -> deep inside a dense region, near a core point.
    center = X[y == y[0]].mean(axis=0)
    result = fitted_clusterer.assign(center)

    assert result.labels[0] >= 0
    assert result.confidences[0] > 0.0
    assert bool(result.anomalies[0]) is False


def test_far_point_is_noise_and_anomaly(fitted_clusterer: DBSCANClusterer) -> None:
    """A point far from every blob is labelled -1 (new pattern) and flagged as an anomaly."""
    far = np.full(_N_FEATURES, 100.0)
    result = fitted_clusterer.assign(far)

    assert result.labels[0] == -1
    assert bool(result.anomalies[0]) is True
    assert result.confidences[0] == 0.0


def test_anomalies_are_exactly_noise_labels(fitted_clusterer: DBSCANClusterer) -> None:
    """The anomaly flag is true exactly for the -1 (noise) rows."""
    X, _ = _blobs()
    # Mix in some clearly-anomalous rows so both classes are present.
    far = np.full((3, _N_FEATURES), 100.0)
    batch = np.vstack([X, far])
    result = fitted_clusterer.assign(batch)
    np.testing.assert_array_equal(result.anomalies, result.labels == -1)


# --------------------------------------------------------------------------- #
# Single-vector / shape handling
# --------------------------------------------------------------------------- #


def test_assign_accepts_single_1d_vector(fitted_clusterer: DBSCANClusterer) -> None:
    """assign accepts a single 1-D vector (shape (d,)) -> length-1 result arrays."""
    X, y = _blobs()
    center = X[y == y[0]].mean(axis=0)  # 1-D (d,)
    assert center.ndim == 1

    result = fitted_clusterer.assign(center)
    assert result.labels.shape == (1,)
    assert result.confidences.shape == (1,)
    assert result.anomalies.shape == (1,)


def test_assign_single_row_2d(fitted_clusterer: DBSCANClusterer) -> None:
    """assign also accepts an explicit (1, d) row."""
    row = np.full((1, _N_FEATURES), 100.0)
    result = fitted_clusterer.assign(row)
    assert result.labels.shape == (1,)


# --------------------------------------------------------------------------- #
# Cluster statistics helpers (shared base implementation)
# --------------------------------------------------------------------------- #


def test_cluster_sizes_includes_counts(fitted_clusterer: DBSCANClusterer) -> None:
    """cluster_sizes() maps id->count and the counts sum to the training row count."""
    sizes = fitted_clusterer.cluster_sizes()
    assert isinstance(sizes, dict)
    assert len(sizes) > 0
    assert sum(sizes.values()) == _N_SAMPLES


def test_n_clusters_excludes_noise_bucket(fitted_clusterer: DBSCANClusterer) -> None:
    """n_clusters() counts only non-noise clusters; cluster_sizes() may include -1."""
    sizes = fitted_clusterer.cluster_sizes()
    non_noise = {cid for cid in sizes if cid != -1}
    assert fitted_clusterer.n_clusters() == len(non_noise)
    # The -1 bucket, if present, is excluded from n_clusters() but kept in cluster_sizes().
    if -1 in sizes:
        assert fitted_clusterer.n_clusters() == len(sizes) - 1


def test_stats_empty_before_fit() -> None:
    """Before any fit the statistics helpers are empty/zero rather than raising."""
    clusterer = DBSCANClusterer(_blob_config())
    assert clusterer.n_clusters() == 0
    assert clusterer.cluster_sizes() == {}


# --------------------------------------------------------------------------- #
# Refit
# --------------------------------------------------------------------------- #


def test_refit_updates_and_stays_fitted(fitted_clusterer: DBSCANClusterer) -> None:
    """refit on a fresh blob set runs without error and keeps the model working/fitted."""
    X2, _ = _blobs(random_state=7)
    fitted_clusterer.refit(X2)
    assert fitted_clusterer.is_fitted is True
    assert fitted_clusterer.n_clusters() >= 2

    result = fitted_clusterer.assign(X2)
    assert result.labels.shape == (_N_SAMPLES,)
    assert np.all(np.isfinite(result.confidences))


# --------------------------------------------------------------------------- #
# Error handling & edge cases
# --------------------------------------------------------------------------- #


def test_assign_before_fit_raises() -> None:
    """Calling assign before warm_fit/refit raises a clear RuntimeError."""
    clusterer = DBSCANClusterer(_blob_config())
    X, _ = _blobs()
    with pytest.raises(RuntimeError, match="before fit"):
        clusterer.assign(X)


def test_warm_fit_empty_raises() -> None:
    """Fitting on an empty matrix raises ValueError."""
    clusterer = DBSCANClusterer(_blob_config())
    with pytest.raises(ValueError):
        clusterer.warm_fit(np.empty((0, _N_FEATURES)))


def test_all_noise_fit_assigns_all_minus_one() -> None:
    """A fit with no core points (tiny eps) makes assign return all -1 / anomalies, no crash."""
    # eps this small means no point has >= min_samples neighbours -> zero core points.
    clusterer = DBSCANClusterer(_blob_config(eps=1e-6, min_samples=_MIN_SAMPLES))
    X, _ = _blobs()
    clusterer.warm_fit(X)

    # Every training row was noise -> no real clusters.
    assert clusterer.n_clusters() == 0

    result = clusterer.assign(X)
    assert result.labels.shape == (_N_SAMPLES,)
    assert np.all(result.labels == -1)
    assert np.all(result.anomalies)
    assert np.all(result.confidences == 0.0)
    assert np.all(np.isfinite(result.confidences))
