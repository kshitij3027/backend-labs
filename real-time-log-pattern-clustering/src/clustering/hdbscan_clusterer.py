"""HDBSCAN clusterer — the density-based, hierarchical :class:`Clusterer` (C7).

HDBSCAN gives the engine a *variable-density* view of the log feature space: unlike
K-means it discovers the number of clusters itself and freely labels points ``-1``
("noise") when they fit no dense region — exactly the project's "new pattern / anomaly"
signal (requirements §2). It is the most expressive of the three algorithms because it
builds a cluster *hierarchy* and condenses it, so clusters of differing densities coexist.

Why the standalone ``hdbscan`` package (not ``sklearn.cluster.HDBSCAN``)
-----------------------------------------------------------------------
The streaming engine's hot path (:meth:`assign`) must score new logs against an *already
fitted* model without re-clustering. The standalone ``hdbscan`` package exposes
:func:`hdbscan.approximate_predict`, which assigns new points to the existing condensed
tree in roughly O(query) time — sklearn's ``HDBSCAN`` has no equivalent ``predict``. For
that function to work the model must be fit with ``prediction_data=True`` (it caches the
extra structures ``approximate_predict`` needs); we therefore always pass that flag and
build a *fresh* model on every fit/refit so the prediction data stays consistent.

Confidence & anomaly model
--------------------------
``hdbscan.approximate_predict`` returns, per query point, a ``label`` (``-1`` == noise) and
a ``strength`` — the membership probability of that point in its assigned cluster, already
in ``[0, 1]``. We map:

* **confidence** = ``clip(nan_to_num(strength), 0, 1)`` — a point deep inside a dense
  cluster gets a near-1 strength; a marginal/edge point gets a low one. Noise points get
  ``0``.
* **anomaly** = ``label == -1`` — the point belongs to no cluster, i.e. it is a brand-new /
  outlier pattern (density-based anomaly, the natural HDBSCAN analogue of K-means' distance
  rule). This is the primary, robust signal and is what the tests pin.

All public methods are numerically defensive: empty / short / 1-D inputs are handled,
confidences are clipped to ``[0, 1]`` with no ``NaN`` ever emitted, and a too-small fit
batch transiently relaxes ``min_cluster_size`` so HDBSCAN never raises on a tiny window.
The fitted model + recorded training labels are picklable for later model save.
"""

from __future__ import annotations

import numpy as np

import hdbscan

from src.clustering.base import Clusterer, ClusterResult
from src.config import AppConfig, load_config

#: HDBSCAN requires ``min_cluster_size >= 2``; this is the floor we relax down to when a fit
#: batch is too small to support the configured value.
_MIN_CLUSTER_SIZE_FLOOR: int = 2


class HDBSCANClusterer(Clusterer):
    """Streaming HDBSCAN over the dense feature matrix, with confidence + anomaly scoring.

    Construct with an :class:`AppConfig` (or let it load the default), call
    :meth:`warm_fit` on the warm-up batch, then :meth:`assign` per micro-batch on the hot
    path (via :func:`hdbscan.approximate_predict`) and :meth:`refit` periodically on the
    sliding window. The model is created lazily inside the fit so each (re)fit gets a fresh
    ``HDBSCAN(prediction_data=True)`` whose cached prediction data matches the latest data.
    """

    name = "hdbscan"

    def __init__(self, config: AppConfig | None = None) -> None:
        """Capture the HDBSCAN hyperparameters from ``config`` (model built lazily on fit).

        Reads ``hdbscan.min_cluster_size`` (default 10) and ``hdbscan.min_samples``
        (default 5). The model itself is **not** created here — it is constructed in
        :meth:`warm_fit` / :meth:`refit` so every fit yields a fresh, prediction-ready
        estimator. Call :meth:`warm_fit` before :meth:`assign`.

        Args:
            config: Application config. When ``None``, :func:`src.config.load_config` is used.
        """
        self.config: AppConfig = config if config is not None else load_config()

        hp = self.config.hdbscan
        # Honour the configured values but never go below HDBSCAN's hard minimum of 2.
        self._min_cluster_size: int = max(
            _MIN_CLUSTER_SIZE_FLOOR, int(hp.min_cluster_size)
        )
        self._min_samples: int = max(1, int(hp.min_samples))

        self.model: hdbscan.HDBSCAN | None = None
        self._fitted: bool = False
        # Set on every fit/refit; read by the shared n_clusters()/cluster_sizes() helpers.
        self._train_labels: np.ndarray | None = None
        # Kept for reference / downstream inspection (per-training-point membership + outlier
        # strength). Not part of the hot path but cheap to retain.
        self._train_probabilities: np.ndarray | None = None
        self._train_outlier_scores: np.ndarray | None = None

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #

    def warm_fit(self, X: np.ndarray) -> None:
        """Initial fit on the warm-up batch ``X`` (shape ``(n, d)``).

        Builds a fresh ``HDBSCAN(prediction_data=True)`` and fits it, then stores
        ``self._train_labels`` (so the shared statistics helpers work) along with the
        training ``probabilities_`` / ``outlier_scores_`` for reference.

        Args:
            X: Warm-up feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def refit(self, X: np.ndarray) -> None:
        """Re-fit on the sliding-window batch ``X``, refreshing labels + prediction data.

        Builds a brand-new ``HDBSCAN(prediction_data=True)`` (HDBSCAN has no incremental
        update, and a fresh estimator keeps the cached prediction data consistent), so the
        model tracks pattern drift across the stream. Behaves exactly like :meth:`warm_fit`.

        Args:
            X: Sliding-window feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def _fit(self, X: np.ndarray) -> None:
        """Shared fit path for warm_fit/refit: build a fresh model, fit, store labels."""
        X = self._as_2d(X)
        n_samples = X.shape[0]
        if n_samples == 0:
            raise ValueError("HDBSCANClusterer fit requires a non-empty feature matrix")

        # HDBSCAN needs enough points to form a cluster of size ``min_cluster_size``. On a
        # tiny warm-up/window, transiently relax it (>= 2) so the fit never raises.
        effective_min_cluster_size = self._min_cluster_size
        if n_samples < self._min_cluster_size * 2:
            effective_min_cluster_size = max(
                _MIN_CLUSTER_SIZE_FLOOR, min(self._min_cluster_size, n_samples)
            )
        # min_samples must not exceed the available sample count either.
        effective_min_samples = max(1, min(self._min_samples, n_samples))

        self.model = hdbscan.HDBSCAN(
            min_cluster_size=int(effective_min_cluster_size),
            min_samples=int(effective_min_samples),
            prediction_data=True,
        )
        self.model.fit(X)

        self._train_labels = np.asarray(self.model.labels_, dtype=int)
        self._train_probabilities = np.asarray(
            getattr(self.model, "probabilities_", np.zeros(n_samples)), dtype=float
        )
        # outlier_scores_ is only present on some configurations; guard its access.
        outlier_scores = getattr(self.model, "outlier_scores_", None)
        self._train_outlier_scores = (
            np.asarray(outlier_scores, dtype=float)
            if outlier_scores is not None
            else None
        )

        self._fitted = True

    # ------------------------------------------------------------------ #
    # Assigning (hot path)
    # ------------------------------------------------------------------ #

    def assign(self, X: np.ndarray) -> ClusterResult:
        """Assign each row of ``X`` to a cluster with a confidence + anomaly flag.

        Predict-only (no re-fit) via :func:`hdbscan.approximate_predict`, which scores ``X``
        against the existing condensed tree. Accepts a single 1-D vector (shape ``(d,)``) or
        a 2-D batch (shape ``(n, d)``).

        ``label == -1`` means noise — a brand-new / outlier pattern — and is flagged as an
        anomaly. ``confidence`` is the returned membership ``strength`` clipped to ``[0, 1]``
        (noise points get ``0``).

        Args:
            X: Feature vector or matrix to score.

        Returns:
            A :class:`ClusterResult` with length-``n`` ``labels`` (int), ``confidences``
            (float in ``[0, 1]``) and ``anomalies`` (bool).

        Raises:
            RuntimeError: If called before :meth:`warm_fit` / :meth:`refit`.
        """
        if not self._fitted or self.model is None:
            raise RuntimeError(
                "HDBSCANClusterer.assign called before fit; call warm_fit(X) "
                "on a warm-up batch first"
            )

        X = self._as_2d(X)
        n = X.shape[0]
        if n == 0:
            return ClusterResult(
                labels=np.empty(0, dtype=int),
                confidences=np.empty(0, dtype=float),
                anomalies=np.empty(0, dtype=bool),
            )

        # Fast streaming assignment against the fitted condensed tree. Returns parallel
        # arrays: ``labels`` (-1 == noise) and ``strengths`` (membership prob in [0, 1]).
        labels, strengths = hdbscan.approximate_predict(self.model, X)
        labels = np.asarray(labels, dtype=int)
        strengths = np.asarray(strengths, dtype=float)

        confidences = np.clip(np.nan_to_num(strengths, nan=0.0, posinf=1.0, neginf=0.0), 0.0, 1.0)
        # Density-based anomaly: noise points belong to no cluster -> new pattern / outlier.
        anomalies = labels == -1

        return ClusterResult(
            labels=labels.astype(int),
            confidences=confidences.astype(float),
            anomalies=anomalies.astype(bool),
        )

    @property
    def is_fitted(self) -> bool:
        """``True`` once :meth:`warm_fit` or :meth:`refit` has run."""
        return self._fitted

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _as_2d(X: np.ndarray) -> np.ndarray:
        """Coerce input to a float64 ``(n, d)`` matrix; a 1-D vector becomes a single row."""
        arr = np.asarray(X, dtype=np.float64)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        return arr


__all__ = ["HDBSCANClusterer"]
