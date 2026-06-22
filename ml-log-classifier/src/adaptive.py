"""Adaptive learning loop: an in-process drift monitor (Commit 12, Feature Area B).

This module owns the *"is the live model still good enough?"* question. It does
**one** thing: track how often the served model's **severity** prediction agrees
with the ground-truth label that ops feeds back via ``POST /feedback``, over a
rolling window, and decide when that agreement has dropped far enough to warrant
an automatic retrain.

The retrain itself (corpus generation, fitting, the atomic hot-swap, registry
version bump and graceful fallback) is **not** here — it is reused wholesale from
the existing background-training machinery in :mod:`src.api`. This class is the
pure, side-effect-free *signal*; the API layer reads :meth:`should_retrain` and
acts on it. Keeping the policy (when to retrain) separate from the mechanism (how
to retrain) is what lets both be tested in isolation.

Design notes
------------
* **Severity is the monitored axis.** The spec grades "accuracy against
  ground-truth labels from ops", and severity is the headline label the
  classifier exists to get right, so a feedback example counts as *correct* iff
  the predicted severity equals the true severity.
* **A rolling window of correctness bits.** A :class:`collections.deque` capped
  at ``window`` holds a ``1`` (correct) or ``0`` (wrong) per recent feedback. The
  recent accuracy is just the mean of that window. Old feedback ages out
  automatically as new feedback arrives, so the signal tracks *recent* drift
  rather than lifetime accuracy.
* **No evidence ⇒ no drift.** An empty window reports ``recent_accuracy == 1.0``
  and :meth:`should_retrain` only fires once the window is **full** *and* below
  the threshold. This guarantees a fresh service never triggers a retrain on a
  single early mistake — we wait until there is a statistically meaningful sample.
* **Re-arming after a retrain.** :meth:`mark_retrained` clears the window so the
  monitor re-evaluates the *new* model from scratch. Without this a single dip
  would keep re-triggering retrains (the old bad bits would linger in the window
  even after a fresh model swapped in).
* **Thread-safe.** ``POST /feedback`` runs as a synchronous handler in FastAPI's
  worker threadpool, so several requests can call :meth:`record` concurrently. A
  single :class:`threading.Lock` guards every read and write of the window and the
  counters, so a reader never observes a torn update and two writers never lose an
  append.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any, Deque


class DriftMonitor:
    """Rolling-window accuracy monitor that decides when to auto-retrain.

    Tracks the served model's severity correctness over the last ``window``
    feedback submissions and exposes a single policy decision —
    :meth:`should_retrain` — that the API layer consults after recording each
    piece of feedback.

    All state is guarded by an internal :class:`threading.Lock`, so every method
    is safe to call concurrently from FastAPI's threadpool.

    Attributes:
        window: The maximum number of recent feedback bits considered (the
            ``deque``'s ``maxlen``). Read-only after construction.
        threshold: The recent-accuracy floor; once the window is full and the
            mean correctness drops **below** this, :meth:`should_retrain` fires.
        total_feedback: Lifetime count of feedback submissions recorded (never
            reset, even by :meth:`mark_retrained`).
        retrains_triggered: Lifetime count of retrains this monitor has signalled
            (incremented by :meth:`mark_retrained`).
    """

    def __init__(self, window: int = 100, threshold: float = 0.90) -> None:
        """Create a monitor over a rolling window of the last ``window`` feedbacks.

        Args:
            window: Maximum number of recent correctness bits to retain. Coerced
                to at least 1 so the ``deque`` always has a positive capacity.
            threshold: Recent-accuracy floor in ``[0, 1]``; ``should_retrain``
                fires when the (full) window's mean correctness is strictly below
                this. Defaults to ``0.90`` (the spec's
                ``accuracy_retrain_threshold``).
        """
        self.window: int = max(1, int(window))
        self.threshold: float = float(threshold)
        self._bits: Deque[int] = deque(maxlen=self.window)
        self.total_feedback: int = 0
        self.retrains_triggered: int = 0
        self._lock = threading.Lock()

    def record(self, predicted_severity: str, actual_severity: str) -> bool:
        """Record one feedback example and return whether it was correct.

        Appends ``1`` to the window if the predicted severity matches the
        ground-truth severity, else ``0`` (the oldest bit ages out automatically
        once the window is full), and bumps :attr:`total_feedback`.

        Args:
            predicted_severity: The severity the live model assigned to the log.
            actual_severity: The ground-truth severity supplied by ops.

        Returns:
            ``True`` if the prediction matched the ground truth, else ``False``.
        """
        correct = str(predicted_severity) == str(actual_severity)
        with self._lock:
            self._bits.append(1 if correct else 0)
            self.total_feedback += 1
        return correct

    def recent_accuracy(self) -> float:
        """Return the mean correctness over the current window.

        An **empty** window returns ``1.0`` — "no evidence of drift yet" — so a
        brand-new service never looks like it is failing before any feedback has
        arrived.

        Returns:
            The fraction of recent feedbacks that were correct, in ``[0, 1]``.
        """
        with self._lock:
            if not self._bits:
                return 1.0
            return sum(self._bits) / len(self._bits)

    def is_window_full(self) -> bool:
        """Return ``True`` once the window holds a full ``window`` samples."""
        with self._lock:
            return len(self._bits) >= self.window

    def should_retrain(self) -> bool:
        """Return ``True`` when a retrain is warranted.

        The decision is intentionally conservative: it fires **only** when the
        window is full *and* the recent accuracy is strictly below
        :attr:`threshold`. Requiring a full window means the trigger reflects a
        meaningful sample rather than a single unlucky prediction.

        Returns:
            ``True`` if the (full) recent accuracy is below the threshold.
        """
        with self._lock:
            if len(self._bits) < self.window:
                return False
            accuracy = sum(self._bits) / len(self._bits)
            return accuracy < self.threshold

    def mark_retrained(self) -> None:
        """Record that a retrain was triggered and re-arm the monitor.

        Increments :attr:`retrains_triggered` and **clears** the window so the
        monitor evaluates the freshly-swapped model from scratch. Clearing is what
        prevents the same dip from immediately re-triggering another retrain: the
        stale low-accuracy bits that justified this retrain are discarded, and the
        next ``window`` feedbacks are gathered against the new model before
        :meth:`should_retrain` can fire again.
        """
        with self._lock:
            self.retrains_triggered += 1
            self._bits.clear()

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-safe snapshot of the monitor's current state.

        Taken under the lock so the reported accuracy, sizes and flags are all
        mutually consistent (never a mix from before and after a concurrent
        :meth:`record`).

        Returns:
            A dict with::

                {
                  "recent_accuracy":    float,   # mean of the window (1.0 if empty)
                  "window_size":        int,     # current number of bits held
                  "window_capacity":    int,     # the configured window (maxlen)
                  "threshold":          float,   # retrain accuracy floor
                  "total_feedback":     int,     # lifetime feedbacks recorded
                  "retrains_triggered": int,     # lifetime retrains signalled
                  "is_window_full":     bool,    # window_size == window_capacity
                }
        """
        with self._lock:
            size = len(self._bits)
            accuracy = (sum(self._bits) / size) if size else 1.0
            return {
                "recent_accuracy": float(accuracy),
                "window_size": int(size),
                "window_capacity": int(self.window),
                "threshold": float(self.threshold),
                "total_feedback": int(self.total_feedback),
                "retrains_triggered": int(self.retrains_triggered),
                "is_window_full": size >= self.window,
            }
