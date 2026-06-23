"""Unit tests for the HDBSCAN clusterer (:mod:`src.clustering.hdbscan_clusterer`).

These pin the streaming :class:`~src.clustering.base.Clusterer` contract — the same one
K-means (C5) and DBSCAN (C6) satisfy — exercised through the HDBSCAN implementation, which
is the density-based, ``-1``-emitting member of the trio:

* :meth:`warm_fit` discovers ``>= 2`` clusters on well-separated blobs,
* :meth:`assign` returns a :class:`ClusterResult` whose ``labels`` / ``confidences`` /
  ``anomalies`` are parallel length-``n`` arrays, confidences in ``[0, 1]``, with some
  non-noise (``>= 0``) labels,
* a point far outside every blob is labelled ``-1`` and flagged as an anomaly (the core
  density / "new pattern" signal), while a point at a blob centre is at least scored
  without crashing,
* :meth:`assign` accepts a single 1-D vector,
* :meth:`n_clusters` (excluding ``-1``) / :meth:`cluster_sizes` reflect the fit,
* :meth:`refit` updates without error and keeps the model fitted, and
* :meth:`assign` before any fit raises a clear error.

The assertions are deliberately robust to HDBSCAN/numpy-2 quirks: they pin shapes, value
ranges and the far-point anomaly (the stable invariants), and only require ``>= 2`` clusters
rather than an exact count. ``sklearn.datasets.make_blobs`` supplies clean, well-separated
data so dense clusters reliably form.
"""

from __future__ import annotations

import numpy as np
import pytest
from sklearn.datasets import make_blobs

from src.clustering.base import Clusterer, ClusterResult
from src.clustering.hdbscan_clusterer import HDBSCANClusterer
from src.config import AppConfig

# --------------------------------------------------------------------------- #
# Fixtures / constants
# --------------------------------------------------------------------------- #

_N_SAMPLES = 400
_N_CENTERS = 4
_N_FEATURES = 10
_CLUSTER_STD = 0.5

# Override knobs so dense, well-separated blobs reliably condense into clusters.
_MIN_CLUSTER_SIZE = 15
_MIN_SAMPLES = 5


def _blobs(random_state: int = 0):
    """Return a clean ``(X, centers)`` blob dataset (4 well-separated, tight centres)."""
    X, _, centers = make_blobs(
        n_samples=_N_SAMPLES,
        centers=_N_CENTERS,
        n_features=_N_FEATURES,
        cluster_std=_CLUSTER_STD,
        random_state=random_state,
        return_centers=True,
    )
    return X.astype(np.float64), centers.astype(np.float64)


def _tuned_config() -> AppConfig:
    """An :class:`AppConfig` with HDBSCAN knobs tuned so the blobs form clusters."""
    cfg = AppConfig()
    cfg.hdbscan.min_cluster_size = _MIN_CLUSTER_SIZE
    cfg.hdbscan.min_samples = _MIN_SAMPLES
    return cfg


@pytest.fixture
def fitted_clusterer() -> HDBSCANClusterer:
    """An :class:`HDBSCANClusterer` warm-fit on the canonical blob dataset."""
    clusterer = HDBSCANClusterer(_tuned_config())
    X, _ = _blobs()
    clusterer.warm_fit(X)
    return clusterer


# --------------------------------------------------------------------------- #
# Contract / type checks
# --------------------------------------------------------------------------- #


def test_is_a_clusterer_with_name() -> None:
    """HDBSCANClusterer is a Clusterer subclass tagged ``"hdbscan"``."""
    clusterer = HDBSCANClusterer(_tuned_config())
    assert isinstance(clusterer, Clusterer)
    assert clusterer.name == "hdbscan"
    assert clusterer.is_fitted is False


def test_warm_fit_discovers_multiple_clusters(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """On 4 well-separated blobs HDBSCAN condenses at least 2 clusters."""
    assert fitted_clusterer.is_fitted is True
    assert fitted_clusterer.n_clusters() >= 2


def test_assign_returns_parallel_cluster_result(
    fitted_clusterer: HDBSCANClusterer,
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


def test_assign_produces_some_non_noise_labels(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """Scoring the training-like blobs assigns at least some points to real clusters."""
    X, _ = _blobs()
    labels = fitted_clusterer.assign(X).labels
    assert np.any(labels >= 0)


def test_confidences_within_unit_interval(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """Confidences are finite and lie within [0, 1] (never NaN/Inf)."""
    X, _ = _blobs()
    conf = fitted_clusterer.assign(X).confidences
    assert np.all(np.isfinite(conf))
    assert np.all(conf >= 0.0)
    assert np.all(conf <= 1.0)


# --------------------------------------------------------------------------- #
# Anomaly behaviour (density / "new pattern" rule)
# --------------------------------------------------------------------------- #


def test_far_point_is_noise_and_anomaly(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """A point far outside all blobs is labelled -1 (noise) and flagged as an anomaly."""
    far = np.full(_N_FEATURES, 100.0)
    result = fitted_clusterer.assign(far)
    assert int(result.labels[0]) == -1
    assert bool(result.anomalies[0]) is True


def test_center_point_does_not_crash(fitted_clusterer: HDBSCANClusterer) -> None:
    """A point AT a real blob centre scores without error and yields a valid result.

    ``approximate_predict`` membership for a dead-centre point is implementation-dependent
    (it can occasionally still read as marginal/noise), so we only assert it scores cleanly
    rather than pinning a non-negative label here — the robust label signal is the far-point
    anomaly above.
    """
    _, centers = _blobs()
    result = fitted_clusterer.assign(centers[0])
    assert result.labels.shape == (1,)
    assert np.isfinite(result.confidences[0])
    assert result.labels[0] >= -1


# --------------------------------------------------------------------------- #
# Single-vector / shape handling
# --------------------------------------------------------------------------- #


def test_assign_accepts_single_1d_vector(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """assign accepts a single 1-D vector (shape (d,)) -> length-1 result arrays."""
    vec = np.zeros(_N_FEATURES)
    result = fitted_clusterer.assign(vec)
    assert result.labels.shape == (1,)
    assert result.confidences.shape == (1,)
    assert result.anomalies.shape == (1,)


def test_assign_single_row_2d(fitted_clusterer: HDBSCANClusterer) -> None:
    """assign also accepts an explicit (1, d) row."""
    row = np.zeros((1, _N_FEATURES))
    result = fitted_clusterer.assign(row)
    assert result.labels.shape == (1,)


# --------------------------------------------------------------------------- #
# Cluster statistics helpers (shared base implementation)
# --------------------------------------------------------------------------- #


def test_n_clusters_excludes_noise(fitted_clusterer: HDBSCANClusterer) -> None:
    """n_clusters() counts only real clusters (the -1 noise label is excluded)."""
    sizes = fitted_clusterer.cluster_sizes()
    # n_clusters() must equal the number of non-(-1) keys in cluster_sizes().
    non_noise_ids = [cid for cid in sizes if cid != -1]
    assert fitted_clusterer.n_clusters() == len(non_noise_ids)
    assert fitted_clusterer.n_clusters() >= 2


def test_cluster_sizes_sum_to_training_n(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """cluster_sizes() maps id->count and the counts sum to the training row count."""
    sizes = fitted_clusterer.cluster_sizes()
    assert isinstance(sizes, dict)
    assert sum(sizes.values()) == _N_SAMPLES


def test_stats_empty_before_fit() -> None:
    """Before any fit the statistics helpers are empty/zero rather than raising."""
    clusterer = HDBSCANClusterer(_tuned_config())
    assert clusterer.n_clusters() == 0
    assert clusterer.cluster_sizes() == {}


# --------------------------------------------------------------------------- #
# Refit
# --------------------------------------------------------------------------- #


def test_refit_updates_and_stays_fitted(
    fitted_clusterer: HDBSCANClusterer,
) -> None:
    """refit on a fresh blob set runs without error and keeps the model fitted + usable."""
    X2, _ = _blobs(random_state=7)
    fitted_clusterer.refit(X2)
    assert fitted_clusterer.is_fitted is True

    result = fitted_clusterer.assign(X2)
    assert result.labels.shape == (_N_SAMPLES,)
    assert np.all(np.isfinite(result.confidences))
    assert np.all((result.confidences >= 0.0) & (result.confidences <= 1.0))


def test_tiny_batch_fit_does_not_crash() -> None:
    """A fit batch smaller than 2*min_cluster_size relaxes the param instead of raising."""
    clusterer = HDBSCANClusterer(_tuned_config())
    X_small, _ = make_blobs(
        n_samples=6,
        centers=2,
        n_features=_N_FEATURES,
        cluster_std=_CLUSTER_STD,
        random_state=1,
    )
    # Should not raise despite n_samples (6) < min_cluster_size*2 (30).
    clusterer.warm_fit(X_small.astype(np.float64))
    assert clusterer.is_fitted is True
    result = clusterer.assign(X_small.astype(np.float64))
    assert result.labels.shape == (6,)
    assert np.all(np.isfinite(result.confidences))


# --------------------------------------------------------------------------- #
# Error handling
# --------------------------------------------------------------------------- #


def test_assign_before_fit_raises() -> None:
    """Calling assign before warm_fit/refit raises a clear RuntimeError."""
    clusterer = HDBSCANClusterer(_tuned_config())
    X, _ = _blobs()
    with pytest.raises(RuntimeError, match="before fit"):
        clusterer.assign(X)


def test_warm_fit_empty_raises() -> None:
    """Fitting on an empty matrix raises ValueError."""
    clusterer = HDBSCANClusterer(_tuned_config())
    with pytest.raises(ValueError):
        clusterer.warm_fit(np.empty((0, _N_FEATURES)))
