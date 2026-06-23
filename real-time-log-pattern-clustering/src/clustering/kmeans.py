"""K-means clusterer (``MiniBatchKMeans``) — the first concrete :class:`Clusterer`.

K-means gives the engine a *partitional* view of the log feature space: every log is
assigned to exactly one of ``k`` centroids — it never returns ``-1`` (that "new pattern"
signal is the job of the density algorithms, DBSCAN/HDBSCAN). :class:`MiniBatchKMeans` is
used rather than plain :class:`~sklearn.cluster.KMeans` because the streaming engine refits
on a sliding window every ``realtime.update_interval``; the mini-batch variant keeps that
refit cheap enough to stay off the hot path.

Confidence & anomaly model
--------------------------
On :meth:`warm_fit` we record the distribution of point-to-assigned-centroid distances over
the warm-up batch: ``mean``, ``std`` and an anomaly threshold ``mean + 3·std`` (a ~3-sigma
far-from-centroid rule). At :meth:`assign` time:

* **confidence** = the assigned cluster's probability under a stable softmax over
  ``-D / scale``, where ``D`` is the per-point distance to *every* centroid and
  ``scale = max(dist_mean, 1e-6)``. A point hugging one centroid (small distance there,
  large elsewhere) gets a near-1 confidence; a point equidistant from several centroids
  gets a low one. The value is always in ``(0, 1]``.
* **anomaly** = ``d_assigned > mean + 3·std`` — the point is unusually far from even its
  nearest centroid (project requirements §2: "detect anomalies based on distance metrics
  from cluster centers").

All public methods are numerically defensive: empty / short / 1-D inputs are handled, and
confidences are clipped to ``[0, 1]`` with no ``NaN`` ever emitted. Everything (the fitted
``MiniBatchKMeans`` and the recorded distance stats) is picklable for later model save.
"""

from __future__ import annotations

import numpy as np
from sklearn.cluster import MiniBatchKMeans

from src.clustering.base import Clusterer, ClusterResult, _softmax
from src.config import AppConfig, load_config

#: Floor for the softmax distance scale so we never divide by ~0 when the warm-up batch is
#: pathologically tight (all points on top of their centroid).
_MIN_SCALE: float = 1e-6


class KMeansClusterer(Clusterer):
    """Streaming K-means over the dense feature matrix, with confidence + anomaly scoring.

    Construct with an :class:`AppConfig` (or let it load the default), call
    :meth:`warm_fit` on the warm-up batch, then :meth:`assign` per micro-batch on the hot
    path and :meth:`refit` periodically on the sliding window.
    """

    name = "kmeans"

    def __init__(self, config: AppConfig | None = None) -> None:
        """Build the (unfitted) ``MiniBatchKMeans`` from ``config`` knobs.

        Reads ``kmeans.n_clusters`` / ``kmeans.max_iter`` / ``kmeans.random_state`` and caps
        the cluster count at ``realtime.max_clusters`` (the spec's hard ceiling on live
        clusters). ``batch_size`` comes from ``realtime.batch_size`` (falling back to 256
        when unset/zero). The model is created but **not** fit — call :meth:`warm_fit` first.

        Args:
            config: Application config. When ``None``, :func:`src.config.load_config` is used.
        """
        self.config: AppConfig = config if config is not None else load_config()

        km = self.config.kmeans
        rt = self.config.realtime

        # Cap k at the live-cluster ceiling, but keep at least 1 cluster.
        n_clusters = max(1, min(int(km.n_clusters), int(rt.max_clusters)))
        batch_size = int(rt.batch_size) or 256

        self.model = MiniBatchKMeans(
            n_clusters=n_clusters,
            max_iter=int(km.max_iter),
            random_state=int(km.random_state),
            n_init="auto",
            batch_size=batch_size,
        )

        self._fitted: bool = False
        # Set on every fit/refit; read by the shared n_clusters()/cluster_sizes() helpers.
        self._train_labels: np.ndarray | None = None
        # Distance-distribution stats for confidence scaling + the anomaly threshold.
        self._dist_mean: float = 0.0
        self._dist_std: float = 0.0
        self._anom_threshold: float = float("inf")

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #

    def warm_fit(self, X: np.ndarray) -> None:
        """Initial fit on the warm-up batch ``X`` (shape ``(n, d)``).

        Fits the ``MiniBatchKMeans``, stores ``self._train_labels`` and records the
        distance-to-assigned-centroid statistics (``mean``/``std``/``mean + 3·std``) that
        drive confidence scaling and anomaly detection at assign time.

        Args:
            X: Warm-up feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def refit(self, X: np.ndarray) -> None:
        """Re-fit on the sliding-window batch ``X`` and refresh labels + distance stats.

        A fresh fit with identical parameters (``MiniBatchKMeans`` keeps this fast), so the
        model tracks pattern drift across the stream. Behaves exactly like :meth:`warm_fit`.

        Args:
            X: Sliding-window feature matrix. Must have at least one row.

        Raises:
            ValueError: If ``X`` is empty.
        """
        self._fit(X)

    def _fit(self, X: np.ndarray) -> None:
        """Shared fit path for warm_fit/refit: fit, store labels, record distance stats."""
        X = self._as_2d(X)
        if X.shape[0] == 0:
            raise ValueError("KMeansClusterer fit requires a non-empty feature matrix")

        # MiniBatchKMeans needs n_clusters <= n_samples; transiently shrink k if the batch
        # is tiny so a small warm-up/window never raises. (n_init="auto" handles the rest.)
        n_samples = X.shape[0]
        if self.model.n_clusters > n_samples:
            self.model.set_params(n_clusters=n_samples)

        self.model.fit(X)
        self._train_labels = np.asarray(self.model.labels_, dtype=int)

        # Per-point distance to its assigned centroid over X -> distance distribution.
        d_assigned = self._assigned_distances(X, self._train_labels)
        self._dist_mean = float(np.mean(d_assigned)) if d_assigned.size else 0.0
        self._dist_std = float(np.std(d_assigned)) if d_assigned.size else 0.0
        self._anom_threshold = self._dist_mean + 3.0 * self._dist_std

        self._fitted = True

    # ------------------------------------------------------------------ #
    # Assigning (hot path)
    # ------------------------------------------------------------------ #

    def assign(self, X: np.ndarray) -> ClusterResult:
        """Assign each row of ``X`` to a cluster with a confidence + anomaly flag.

        Predict-only (no re-fit). Accepts a single 1-D vector (shape ``(d,)``) or a 2-D
        batch (shape ``(n, d)``). K-means always returns a real cluster id (never ``-1``).

        Confidence is the assigned cluster's stable-softmax probability over ``-D / scale``
        (``D`` = distance to every centroid, ``scale = max(dist_mean, 1e-6)``); anomaly is
        ``d_assigned > mean + 3·std`` from the warm-up distance distribution.

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
                "KMeansClusterer.assign called before fit; call warm_fit(X) "
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

        labels = np.asarray(self.model.predict(X), dtype=int)

        # Distances to ALL centroids -> D (n, k); pick the assigned one per row.
        D = self.model.transform(X)  # (n, k), Euclidean distance to each centroid
        d_assigned = D[np.arange(n), labels]

        # Stable softmax over -D / scale; confidence = prob mass on the assigned cluster.
        scale = max(self._dist_mean, _MIN_SCALE)
        probs = _softmax(-D / scale)  # (n, k), rows sum to ~1
        confidences = probs[np.arange(n), labels]
        confidences = np.nan_to_num(confidences, nan=0.0, posinf=1.0, neginf=0.0)
        confidences = np.clip(confidences, 0.0, 1.0)

        anomalies = d_assigned > self._anom_threshold

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

    def _assigned_distances(self, X: np.ndarray, labels: np.ndarray) -> np.ndarray:
        """Per-point Euclidean distance from each row of ``X`` to its assigned centroid."""
        D = self.model.transform(X)  # (n, k)
        n = X.shape[0]
        return D[np.arange(n), labels]


__all__ = ["KMeansClusterer"]
