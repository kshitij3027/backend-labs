"""DBSCAN clusterer — the density-based concrete :class:`Clusterer` (C6).

DBSCAN gives the engine a *density* view of the log feature space: dense regions become
clusters and everything sparse is labelled ``-1`` (noise / a brand-new pattern, project
requirements §2). Unlike K-means it never forces every log into a cluster — that ``-1``
escape hatch is exactly the "this log fits no known pattern" signal the anomaly path wants.

Streaming assignment (the nearest-core approximation)
-----------------------------------------------------
:class:`sklearn.cluster.DBSCAN` is **transductive** — it has no ``predict``: a fit only
labels the points it was trained on. To make it usable on the hot path we use the standard
*nearest-core-point* approximation:

* :meth:`warm_fit` runs ``DBSCAN.fit_predict`` once on the warm-up batch, then caches the
  **core points** (``core_sample_indices_``) and their cluster labels. Core points are the
  dense interior of each cluster, so they alone define each cluster's territory.
* :meth:`assign` finds, for each new log, its nearest cached core point via a one-neighbour
  :class:`~sklearn.neighbors.NearestNeighbors` index. If that distance ``d`` is within
  ``eps`` the log joins that core point's cluster; otherwise it is ``-1`` (noise / new
  pattern). This reproduces DBSCAN's own reachability rule (a point belongs to a cluster iff
  it is within ``eps`` of one of that cluster's core points).

Confidence & anomaly model
--------------------------
* **confidence** = ``1 - d / eps`` clipped to ``[0, 1]`` — a log sitting on top of a core
  point (``d ≈ 0``) is near-1, one right at the ``eps`` boundary is ~0, and noise is ``0``.
* **anomaly** = ``label == -1`` — the density algorithms treat every noise point as the
  anomaly / new pattern (density-based, not distance-from-centroid like K-means).

Numerically defensive throughout: empty / short / 1-D inputs are handled, a fit that yields
**zero** core points (e.g. a degenerate tiny-``eps`` window) makes :meth:`assign` return all
``-1`` instead of crashing, ``eps <= 0`` is guarded, and no ``NaN`` is ever emitted. The
fitted index and cached arrays are plain numpy / sklearn objects, so instances are picklable.
"""

from __future__ import annotations

import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors

from src.clustering.base import Clusterer, ClusterResult
from src.config import AppConfig, load_config

#: Floor for ``eps`` when used as the confidence denominator so a pathologically small (or
#: non-positive) configured ``eps`` never divides by ~0. The membership test still uses the
#: real configured ``eps`` (which may legitimately be tiny), so this only guards arithmetic.
_MIN_EPS: float = 1e-12


class DBSCANClusterer(Clusterer):
    """Streaming DBSCAN over the dense feature matrix, via the nearest-core approximation.

    Construct with an :class:`AppConfig` (or let it load the default), call :meth:`warm_fit`
    on the warm-up batch, then :meth:`assign` per micro-batch on the hot path and
    :meth:`refit` periodically on the sliding window. Reads ``dbscan.eps`` and
    ``dbscan.min_samples`` from config.

    Note:
        The default ``dbscan.eps`` (0.3) is tuned for the project's *normalized* feature
        vectors. When clustering raw, larger-scale data (e.g. ``make_blobs`` output) pass a
        config with a correspondingly larger ``eps``.
    """

    name = "dbscan"

    def __init__(self, config: AppConfig | None = None) -> None:
        """Build the (unfitted) ``DBSCAN`` model from ``config`` knobs.

        Reads ``dbscan.eps`` / ``dbscan.min_samples`` and creates a :class:`sklearn.cluster.
        DBSCAN`. The model is created but **not** fit — call :meth:`warm_fit` first.

        Args:
            config: Application config. When ``None``, :func:`src.config.load_config` is used.
        """
        self.config: AppConfig = config if config is not None else load_config()

        db = self.config.dbscan
        self.eps: float = float(db.eps)
        self.min_samples: int = int(db.min_samples)

        self.model = DBSCAN(eps=self.eps, min_samples=self.min_samples)

        self._fitted: bool = False
        # Set on every fit/refit; read by the shared n_clusters()/cluster_sizes() helpers.
        self._train_labels: np.ndarray | None = None
        # Cached core points + their cluster labels (the nearest-core lookup table) and the
        # one-neighbour index over them. ``_has_core`` gates assign when a fit yields none.
        self._core_points: np.ndarray | None = None
        self._core_labels: np.ndarray | None = None
        self._nn: NearestNeighbors | None = None
        self._has_core: bool = False

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #

    def warm_fit(self, X: np.ndarray) -> None:
        """Initial fit on the warm-up batch ``X`` (shape ``(n, d)``).

        Runs ``DBSCAN.fit_predict``, stores ``self._train_labels`` and caches the core points
        + their labels (plus a one-neighbour index over them) for nearest-core assignment.

        Args:
            X: Warm-up feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def refit(self, X: np.ndarray) -> None:
        """Re-fit on the sliding-window batch ``X`` and refresh core points / labels / index.

        A fresh ``fit_predict`` (DBSCAN keeps no incremental state), so the model tracks
        pattern drift across the stream. Behaves exactly like :meth:`warm_fit`.

        Args:
            X: Sliding-window feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def _fit(self, X: np.ndarray) -> None:
        """Shared fit path: fit_predict, store labels, cache core points + neighbour index."""
        X = self._as_2d(X)
        if X.shape[0] == 0:
            raise ValueError("DBSCANClusterer fit requires a non-empty feature matrix")

        labels = np.asarray(self.model.fit_predict(X), dtype=int)
        self._train_labels = labels

        # Core points are the dense interior of each cluster; they alone define cluster
        # territory for the nearest-core assignment rule.
        core_idx = np.asarray(self.model.core_sample_indices_, dtype=int)
        if core_idx.size > 0:
            self._core_points = X[core_idx]
            self._core_labels = labels[core_idx]
            # One-neighbour index for fast nearest-core lookup at assign time.
            self._nn = NearestNeighbors(n_neighbors=1).fit(self._core_points)
            self._has_core = True
        else:
            # All-noise fit (e.g. tiny eps / sparse window): nothing to assign against, so
            # assign() will short-circuit to all -1 rather than query an empty index.
            self._core_points = None
            self._core_labels = None
            self._nn = None
            self._has_core = False

        self._fitted = True

    # ------------------------------------------------------------------ #
    # Assigning (hot path)
    # ------------------------------------------------------------------ #

    def assign(self, X: np.ndarray) -> ClusterResult:
        """Assign each row of ``X`` to a cluster with a confidence + anomaly flag.

        Predict-only (no re-fit). Accepts a single 1-D vector (shape ``(d,)``) or a 2-D batch
        (shape ``(n, d)``). Uses the nearest-core rule: a row joins the cluster of its nearest
        cached core point iff that distance ``d`` is within ``eps``; otherwise it is ``-1``
        (noise / new pattern).

        confidence = ``1 - d / eps`` clipped to ``[0, 1]`` for assigned rows, ``0`` for noise;
        anomaly = ``label == -1``.

        Args:
            X: Feature vector or matrix to score.

        Returns:
            A :class:`ClusterResult` with length-``n`` ``labels`` (int), ``confidences``
            (float in ``[0, 1]``) and ``anomalies`` (bool).

        Raises:
            RuntimeError: If called before :meth:`warm_fit` / :meth:`refit`.
        """
        if not self._fitted:
            raise RuntimeError(
                "DBSCANClusterer.assign called before fit; call warm_fit(X) "
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

        # No core points cached (all-noise fit) -> everything is a new pattern.
        if not self._has_core or self._nn is None or self._core_labels is None:
            return ClusterResult(
                labels=np.full(n, -1, dtype=int),
                confidences=np.zeros(n, dtype=float),
                anomalies=np.ones(n, dtype=bool),
            )

        # Nearest cached core point (distance + index) for every row.
        distances, indices = self._nn.kneighbors(X, n_neighbors=1)
        d = distances[:, 0]
        idx = indices[:, 0]

        # Membership: within eps -> take that core point's cluster, else noise (-1).
        within = d <= self.eps
        labels = np.where(within, self._core_labels[idx], -1).astype(int)

        # confidence = 1 - d/eps on the [0, eps] ramp; 0 for noise. Guard the eps denominator.
        eps_denom = max(self.eps, _MIN_EPS)
        confidences = np.where(within, 1.0 - d / eps_denom, 0.0)
        confidences = np.nan_to_num(confidences, nan=0.0, posinf=1.0, neginf=0.0)
        confidences = np.clip(confidences, 0.0, 1.0)

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


__all__ = ["DBSCANClusterer"]
