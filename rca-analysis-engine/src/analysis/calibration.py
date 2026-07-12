"""Confidence calibration for the RCA Analysis Engine (C9, feature area C).

The confidence scorer (C4) emits a hand-crafted score in ``[0, 1]``, but a raw score of
``0.8`` does not, on its own, mean "80% of such events turn out to be the real root
cause". :class:`ConfidenceCalibrator` closes that gap: it learns a mapping from raw
confidence to *empirical* root-cause probability from **resolved** incidents (each carrying
a known ground-truth root cause) and reports how well-calibrated the engine is via the
**Brier score** and a **reliability diagram**.

The design is deliberately conservative — calibration is a *fidelity upgrade*, never a
correctness risk:

* **Learn from outcomes.** :meth:`record` / :meth:`record_outcome` accumulate
  ``(raw_confidence, was_root_cause)`` pairs. A resolved incident contributes one sample
  per ranked candidate: the true root scores ``1``, every rival ``0``.
* **Fit lazily, only with enough signal.** :meth:`fit` trains a model **only** when there
  are at least ``calibration_min_samples`` samples *and* both outcome classes are present.
  Per ``calibration_method`` it fits either **isotonic regression**
  (:class:`sklearn.isotonic.IsotonicRegression`, ``out_of_bounds="clip"``) or **Platt
  scaling** (:class:`sklearn.linear_model.LogisticRegression`,
  ``predict_proba``). Both are **monotonic non-decreasing** in the raw score.
* **Identity fallback.** Until a model is fitted, :meth:`transform` returns the raw value
  unchanged (clamped to ``[0, 1]``). So a fresh engine — or one with sparse / single-class
  history — behaves exactly as it did pre-C9, and a normal ``analyze()`` can never crash on
  a missing calibrator. Because both fitted transforms are monotonic and the identity is
  too, applying calibration to an already-ranked list **never inverts the ranking** (the
  orchestrator additionally preserves the raw-confidence order and treats the calibrated
  number as a display value).
* **Report calibration quality.** :meth:`stats` returns the method, sample count, fitted
  flag, the raw and calibrated **Brier scores** (via
  :func:`sklearn.metrics.brier_score_loss`, guarded to need both classes — ``None`` when
  not computable), and a 10-bin **reliability diagram** (``mean_predicted`` vs.
  ``observed_freq`` per bin).

Deterministic and free of global state: the same recorded samples always yield the same
fitted model and the same stats.
"""

from __future__ import annotations

import logging

import numpy as np

from src.config import Settings

logger = logging.getLogger(__name__)

__all__ = ["ConfidenceCalibrator"]

#: Number of equal-width bins in the reliability diagram over ``[0, 1]``.
_N_BINS: int = 10

#: Recognized calibration methods; anything else falls back to the first (isotonic).
_ISOTONIC: str = "isotonic"
_PLATT: str = "platt"


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into the inclusive ``[low, high]`` range."""
    return max(low, min(high, value))


def _normalize_method(method: str) -> str:
    """Normalize a configured method string to ``"isotonic"`` / ``"platt"``.

    Unknown / malformed values degrade to isotonic (the safe monotonic default) rather
    than raising, so a typo in config never breaks the engine.
    """
    candidate = (method or "").strip().lower()
    return candidate if candidate in (_ISOTONIC, _PLATT) else _ISOTONIC


class ConfidenceCalibrator:
    """Calibrate raw root-cause confidences against historical incident outcomes."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        #: Recorded raw confidences (parallel to :attr:`_labels`).
        self._raw: list[float] = []
        #: Recorded outcomes (``1`` == was the true root cause, else ``0``).
        self._labels: list[int] = []
        #: Fitted model as ``(kind, estimator)`` where ``kind`` is ``"isotonic"`` /
        #: ``"platt"``, or ``None`` when unfitted (identity transform).
        self._model: tuple[str, object] | None = None

    # --- Recording outcomes ------------------------------------------------------

    def record(self, raw_confidence: float, was_root_cause: bool) -> None:
        """Record one ``(raw_confidence, outcome)`` training sample.

        The raw confidence is clamped into ``[0, 1]`` defensively; the outcome is stored
        as ``1``/``0``. Recording does **not** refit — the caller decides when to
        :meth:`fit` (typically once per resolved incident, see
        ``RCAAnalyzer.record_outcome``).
        """
        self._raw.append(_clamp(float(raw_confidence), 0.0, 1.0))
        self._labels.append(1 if was_root_cause else 0)

    def record_outcome(self, root_causes, true_root_cause_event_id: str) -> None:
        """Record one sample per ranked candidate from a resolved incident.

        For every :class:`~src.models.RootCause` in ``root_causes`` the *pre-calibration*
        score is used (``raw_confidence`` when present — it is what a fitted model must map
        — falling back to ``confidence`` for reports built before C9), labelled ``1`` iff
        the candidate is the known ``true_root_cause_event_id`` and ``0`` otherwise. So a
        single resolved incident contributes both a positive and (usually several) negative
        samples, which is exactly what a calibrator needs to see.
        """
        for rc in root_causes:
            raw = rc.raw_confidence if rc.raw_confidence is not None else rc.confidence
            self.record(raw, rc.event_id == true_root_cause_event_id)

    # --- Fitting -----------------------------------------------------------------

    def fit(self) -> bool:
        """(Re)fit the calibration model from the recorded samples; return ``fitted``.

        Fits **only** when there are at least ``calibration_min_samples`` samples *and*
        both outcome classes are present — otherwise the model is cleared so
        :meth:`transform` stays the identity (a monotonic mapping cannot be learned from a
        single class, and a tiny sample would overfit). Isotonic vs. Platt is selected by
        ``calibration_method``. Idempotent: safe to call after every new batch of outcomes.
        """
        n = len(self._raw)
        if n < self.settings.calibration_min_samples or len(set(self._labels)) < 2:
            self._model = None
            return False

        raw = np.asarray(self._raw, dtype=float)
        method = _normalize_method(self.settings.calibration_method)

        if method == _PLATT:
            from sklearn.linear_model import LogisticRegression

            # Integer 0/1 class labels for the logistic (Platt) classifier.
            estimator = LogisticRegression()
            estimator.fit(raw.reshape(-1, 1), np.asarray(self._labels, dtype=int))
            self._model = (_PLATT, estimator)
        else:  # isotonic (default / fallback)
            from sklearn.isotonic import IsotonicRegression

            # Float targets for the monotonic regression onto the observed frequency.
            estimator = IsotonicRegression(out_of_bounds="clip")
            estimator.fit(raw, np.asarray(self._labels, dtype=float))
            self._model = (_ISOTONIC, estimator)
        return True

    @property
    def fitted(self) -> bool:
        """True once a model has been fitted (``transform`` is no longer the identity)."""
        return self._model is not None

    # --- Applying ----------------------------------------------------------------

    def transform(self, raw: float) -> float:
        """Map a raw confidence to its calibrated probability, clamped to ``[0, 1]``.

        Returns the raw value unchanged (the **identity fallback**) whenever no model is
        fitted, so callers never need to branch on calibrator readiness. Both fitted
        transforms are monotonic non-decreasing, so ``transform`` preserves ordering.
        """
        value = _clamp(float(raw), 0.0, 1.0)
        if self._model is None:
            return value
        kind, estimator = self._model
        if kind == _PLATT:
            calibrated = float(estimator.predict_proba(np.asarray([[value]], dtype=float))[0, 1])
        else:
            calibrated = float(estimator.predict(np.asarray([value], dtype=float))[0])
        return _clamp(calibrated, 0.0, 1.0)

    # --- Reporting ---------------------------------------------------------------

    def stats(self) -> dict:
        """Return calibration diagnostics (method, sample count, Brier, reliability bins).

        Shape::

            {
              "method": "isotonic" | "platt",
              "n_samples": int,
              "fitted": bool,
              "brier_raw": float | None,          # None when not computable (one class)
              "brier_calibrated": float | None,   # None when unfitted or not computable
              "reliability_bins": [
                 {"bin_lower", "bin_upper", "mean_predicted", "observed_freq", "count"}, ...
              ],
            }

        The Brier scores are guarded — :func:`sklearn.metrics.brier_score_loss` needs both
        outcome classes present, so they are ``None`` otherwise. The reliability bins are
        computed over the calibrated predictions when fitted, else the raw scores (empty
        only when no samples have been recorded).
        """
        n = len(self._raw)
        method = _normalize_method(self.settings.calibration_method)
        result: dict = {
            "method": method,
            "n_samples": n,
            "fitted": self.fitted,
            "brier_raw": None,
            "brier_calibrated": None,
            "reliability_bins": [],
        }
        if n == 0:
            return result

        raw = np.asarray(self._raw, dtype=float)
        labels = np.asarray(self._labels, dtype=int)
        both_classes = len(set(self._labels)) >= 2

        if both_classes:
            from sklearn.metrics import brier_score_loss

            result["brier_raw"] = float(brier_score_loss(labels, raw, pos_label=1))

        if self.fitted:
            calibrated = np.asarray([self.transform(value) for value in raw], dtype=float)
            if both_classes:
                from sklearn.metrics import brier_score_loss

                result["brier_calibrated"] = float(
                    brier_score_loss(labels, calibrated, pos_label=1)
                )
            probs = calibrated
        else:
            probs = raw

        result["reliability_bins"] = self._reliability_bins(probs, labels)
        return result

    def _reliability_bins(self, probs: np.ndarray, labels: np.ndarray) -> list[dict]:
        """Manual 10-bin reliability diagram over ``[0, 1]``.

        Each bin reports its edges, the mean predicted probability and the observed
        positive frequency of the samples that fell into it, and the sample ``count``.
        Empty bins carry ``None`` for the two rates (and ``count == 0``) so the returned
        shape is always the full, stable 10-element ladder — the dashboard can plot it
        directly against the diagonal.
        """
        edges = np.linspace(0.0, 1.0, _N_BINS + 1)
        # digitize against the interior edges -> index in 0.._N_BINS-1; the final bin is
        # closed on the right so a probability of exactly 1.0 lands in the last bin.
        idx = np.clip(np.digitize(probs, edges[1:-1], right=False), 0, _N_BINS - 1)
        bins: list[dict] = []
        for b in range(_N_BINS):
            mask = idx == b
            count = int(mask.sum())
            bins.append(
                {
                    "bin_lower": float(edges[b]),
                    "bin_upper": float(edges[b + 1]),
                    "mean_predicted": float(probs[mask].mean()) if count else None,
                    "observed_freq": float(labels[mask].mean()) if count else None,
                    "count": count,
                }
            )
        return bins
