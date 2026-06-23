"""Shared clusterer contract consumed by the streaming engine and every algorithm.

This module defines the *one* interface that the three concrete clusterers — K-means
(C5, :mod:`src.clustering.kmeans`), DBSCAN (C6) and HDBSCAN (C7) — implement and that the
real-time engine (C8) drives. The design is deliberately **streaming-shaped**:

* :meth:`Clusterer.warm_fit` runs **once** on the historical warm-up batch (project
  requirements §2: "fit an initial model on historical/batch data before streaming").
* :meth:`Clusterer.assign` is the **hot path** — predict-only, no re-fit — and must accept
  anywhere from a single row up to a full micro-batch. It returns, per log, a cluster id
  (``-1`` == noise / *new pattern*, §2), a confidence in ``[0, 1]`` and an anomaly flag.
* :meth:`Clusterer.refit` is called periodically (every ``realtime.update_interval``) on a
  sliding window so the model tracks drift without blocking the stream.

A :class:`ClusterResult` is the uniform return type so the engine can fan one log out to
all three algorithms and compare their answers column-for-column. The concrete helpers
(:meth:`Clusterer.n_clusters`, :meth:`Clusterer.cluster_sizes`) are implemented here and
work for *any* subclass purely off the ``self._train_labels`` array each one stores on
fit/refit — subclasses never re-implement them.

The contract is intentionally tiny and free of sklearn types so it stays picklable and the
engine has no idea which algorithm produced a given :class:`ClusterResult`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np


@dataclass
class ClusterResult:
    """Per-log clustering outcome for one algorithm over an ``X`` of shape ``(n, d)``.

    All three arrays are parallel and length ``n`` (one entry per input row):

    Attributes:
        labels: ``int`` cluster ids, shape ``(n,)``. ``-1`` denotes noise / a brand-new
            pattern that fits no existing cluster (project requirements §2). Density
            algorithms (DBSCAN/HDBSCAN) emit ``-1`` freely; K-means never does.
        confidences: ``float`` assignment confidence in ``[0, 1]``, shape ``(n,)``. Higher
            means the point sits more decisively inside its assigned cluster. Always finite
            (never ``NaN``/``Inf``).
        anomalies: ``bool`` outlier flags, shape ``(n,)``. ``True`` means the point is
            unusually far from / sparse around its cluster (distance- or density-based,
            depending on the algorithm).
    """

    labels: np.ndarray  # shape (n,), int; -1 == noise / new pattern
    confidences: np.ndarray  # shape (n,), float in [0, 1]
    anomalies: np.ndarray  # shape (n,), bool


class Clusterer(ABC):
    """Abstract streaming clusterer: warm-fit once, assign on the hot path, refit on a window.

    Subclasses set :pyattr:`name` (``"kmeans"`` | ``"dbscan"`` | ``"hdbscan"``) and implement
    the four abstract members. They MUST store the training labels of their most recent fit
    on ``self._train_labels`` (a ``(n_train,)`` int array, or ``None`` before any fit) so the
    concrete :meth:`n_clusters` / :meth:`cluster_sizes` helpers work uniformly.

    All :meth:`assign` implementations must tolerate a single 1-D feature vector (shape
    ``(d,)``) as well as a 2-D batch (shape ``(n, d)``); use :meth:`_as_2d` to normalize.
    """

    #: Algorithm tag set by each subclass; used by the engine for labelling results.
    name: str = "base"

    #: Training labels from the most recent ``warm_fit`` / ``refit`` (``None`` until fitted).
    #: Subclasses assign this; the shared statistics helpers read it.
    _train_labels: np.ndarray | None = None

    @abstractmethod
    def warm_fit(self, X: np.ndarray) -> None:
        """Perform the initial fit on the historical warm-up batch ``X`` (shape ``(n, d)``)."""
        ...

    @abstractmethod
    def assign(self, X: np.ndarray) -> ClusterResult:
        """Predict-only assignment for ``X``; MUST accept 1..N rows. No re-fitting happens."""
        ...

    @abstractmethod
    def refit(self, X: np.ndarray) -> None:
        """Re-fit the model on the current sliding-window batch ``X`` (periodic, fast)."""
        ...

    @property
    @abstractmethod
    def is_fitted(self) -> bool:
        """Whether the model has been fit (``warm_fit`` or ``refit`` has run)."""
        ...

    # ------------------------------------------------------------------ #
    # Concrete helpers shared by every clusterer
    # ------------------------------------------------------------------ #

    def n_clusters(self) -> int:
        """Number of distinct **non-noise** clusters from the most recent fit.

        Derived from ``self._train_labels`` (set by the subclass on fit/refit), so it is
        identical logic across algorithms. The ``-1`` (noise) label is excluded. Returns
        ``0`` before any fit.
        """
        labels = self._train_labels
        if labels is None or len(labels) == 0:
            return 0
        unique = np.unique(np.asarray(labels))
        return int(np.count_nonzero(unique != -1))

    def cluster_sizes(self) -> dict[int, int]:
        """Map ``cluster_id -> count`` over the most recent training labels.

        Includes the ``-1`` (noise) bucket when present so callers can see how much of the
        training window was unclustered. Returns an empty dict before any fit. The counts
        sum to the number of training rows.
        """
        labels = self._train_labels
        if labels is None or len(labels) == 0:
            return {}
        ids, counts = np.unique(np.asarray(labels), return_counts=True)
        return {int(cid): int(cnt) for cid, cnt in zip(ids, counts)}


def _softmax(neg_distances: np.ndarray) -> np.ndarray:
    """Numerically-stable row-wise softmax over ``neg_distances`` (shape ``(n, k)``).

    Subtracts each row's max before exponentiating so large magnitudes never overflow, and
    floors the denominator so an all-``-inf`` / degenerate row yields a finite uniform
    distribution instead of ``NaN``. Returns a ``(n, k)`` array whose rows sum to ~1.

    Pass the *negated* distances to each cluster (``-D``, optionally scaled): the closest
    cluster has the least-negative value and therefore the largest probability.
    """
    z = np.asarray(neg_distances, dtype=np.float64)
    if z.ndim == 1:
        z = z.reshape(1, -1)
    z = np.nan_to_num(z, nan=0.0, posinf=0.0, neginf=-1e30)
    z = z - np.max(z, axis=1, keepdims=True)
    exp = np.exp(z)
    denom = np.sum(exp, axis=1, keepdims=True)
    # Guard a degenerate all-zero row (shouldn't happen post-shift, but be safe).
    denom = np.where(denom > 0.0, denom, 1.0)
    return exp / denom


__all__ = ["Clusterer", "ClusterResult"]
