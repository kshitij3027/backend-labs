"""Adaptive threshold that auto-tunes based on false-positive feedback."""
from __future__ import annotations

import threading


class AdaptiveThreshold:
    """Dynamically adjusts the anomaly confidence threshold.

    The threshold moves up when the observed false-positive rate exceeds the
    target (reducing sensitivity) and moves down when the FPR is well below
    target (increasing sensitivity).  An EWMA smoother prevents large swings
    from individual observations.

    An optional *load factor* lets callers relax the threshold during high
    traffic (factor > 1) or tighten it during quiet periods (factor < 1).

    Operator feedback (``operator_feedback``) provides an additional manual
    correction signal: confirmed true positives nudge the threshold down and
    dismissed false positives nudge it up.

    All public methods are thread-safe.

    Args:
        initial_threshold: Starting anomaly threshold.
        alpha: EWMA smoothing factor (higher = more reactive).
        target_fpr: Desired false-positive rate.
        adjustment_step: How much to adjust per update cycle.
        min_threshold: Lower bound for the threshold.
        max_threshold: Upper bound for the threshold.
    """

    def __init__(
        self,
        initial_threshold: float = 0.7,
        alpha: float = 0.1,
        target_fpr: float = 0.05,
        adjustment_step: float = 0.02,
        min_threshold: float = 0.3,
        max_threshold: float = 0.95,
    ) -> None:
        self._current_threshold: float = initial_threshold
        self._ewma_fpr: float = 0.0
        self._alpha: float = alpha
        self._target_fpr: float = target_fpr
        self._adjustment_step: float = adjustment_step
        self._min_threshold: float = min_threshold
        self._max_threshold: float = max_threshold
        self._load_factor: float = 1.0
        self._total_updates: int = 0
        self._feedback_history: list[dict] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, was_flagged: bool, was_true_anomaly: bool) -> None:
        """Incorporate one observation and adjust the threshold.

        Args:
            was_flagged: Whether the engine flagged this entry as anomalous.
            was_true_anomaly: Ground-truth label for the entry.
        """
        # Instantaneous FPR: 1.0 if false positive, 0.0 otherwise
        instant_fpr = 1.0 if (was_flagged and not was_true_anomaly) else 0.0

        with self._lock:
            # Update EWMA
            self._ewma_fpr = (
                self._alpha * instant_fpr
                + (1 - self._alpha) * self._ewma_fpr
            )

            # Adjust threshold based on EWMA vs target
            if self._ewma_fpr > self._target_fpr:
                # Too many false positives -> raise threshold (less sensitive)
                self._current_threshold += self._adjustment_step
            elif self._ewma_fpr < self._target_fpr * 0.5:
                # FPR well below target -> lower threshold (more sensitive)
                self._current_threshold -= self._adjustment_step

            # Apply load factor and clamp
            effective = self._current_threshold * self._load_factor
            self._current_threshold = max(
                self._min_threshold,
                min(self._max_threshold, effective),
            )

            self._total_updates += 1

    def get_threshold(self) -> float:
        """Return the current effective threshold (with load factor, clamped)."""
        with self._lock:
            effective = self._current_threshold * self._load_factor
            return max(self._min_threshold, min(self._max_threshold, effective))

    def set_load_factor(self, factor: float) -> None:
        """Set a load multiplier (e.g. 1.2 during high traffic to relax)."""
        with self._lock:
            self._load_factor = factor

    def operator_feedback(self, anomaly_id: str, confirmed: bool) -> None:
        """Record operator feedback and nudge the threshold accordingly.

        Args:
            anomaly_id: Identifier for the anomaly being reviewed.
            confirmed: ``True`` if the operator confirms it is a real anomaly,
                ``False`` if the operator dismisses it as a false positive.
        """
        with self._lock:
            self._feedback_history.append(
                {"anomaly_id": anomaly_id, "confirmed": confirmed}
            )

            if not confirmed:
                # False positive reported -> raise threshold
                self._current_threshold += self._adjustment_step
            else:
                # True positive confirmed -> lower threshold
                self._current_threshold -= self._adjustment_step

            # Clamp
            self._current_threshold = max(
                self._min_threshold,
                min(self._max_threshold, self._current_threshold),
            )

    def get_stats(self) -> dict:
        """Return a snapshot of adaptive threshold state."""
        with self._lock:
            return {
                "current_threshold": self._current_threshold,
                "ewma_fpr": self._ewma_fpr,
                "load_factor": self._load_factor,
                "total_updates": self._total_updates,
                "feedback_count": len(self._feedback_history),
            }
