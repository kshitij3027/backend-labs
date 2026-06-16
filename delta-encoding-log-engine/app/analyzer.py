"""Thin, read-only adaptive recommender for keyframe interval + compression mode.

This module observes the *shape* of the data flowing through the engine and **reports
a recommendation** — it never touches the codec, the store, or how any batch was
compressed. The live encoder's behaviour (and therefore the byte-accounting output)
is byte-identical whether or not the analyzer is present; it only watches.

**What it measures — per-step field churn.** A structured log stream's compressibility
is governed by how much each entry differs from the one before it. For each consecutive
pair ``(prev, cur)`` in an observed batch, the analyzer computes the **step churn**:

    churn = |{keys added, removed, or changed}| / |keys(prev) ∪ keys(cur)|

mirroring the codec's field-level semantics exactly (see :func:`app.codec.diff_entries`):
a key counts as churned iff it is present in only one of the two entries, or present in
both with unequal values (membership + ``!=``, never ``dict.get``). These per-step
fractions accumulate in a bounded :class:`collections.deque` (the sliding window), so
the analyzer's view is always the most recent ``window`` steps.

**What it recommends.** From the windowed mean churn it derives two suggestions:

* a **keyframe interval** that is a monotonically *non-increasing* function of churn —
  low churn (entries barely move) ⇒ near ``max_interval`` (cheap to store, fewer
  keyframes); high churn ⇒ near ``min_interval`` (more keyframes for tighter random
  access). Linear interpolation: ``min + (max - min) * (1 - churn)``.
* a **compression mode** (:class:`CompressionMode`): ``MAX`` when churn is low,
  ``FAST`` when high, ``BALANCED`` in the middle.

Both are advisory. The recommendation rides additively in ``/api/stats`` under an
``analyzer`` section and on each dashboard WebSocket tick; nothing reads it back into
the encoder.

**Thread-safety.** The compress handler runs in Starlette's threadpool and calls
:meth:`observe`; the stats handler / dashboard loop call :meth:`snapshot`. A single
:class:`threading.Lock` guards the deque so concurrent observe/read never races.

~80 lines, read-only, additive (see *plan.md → "Adaptive (thin recommender …)"*).
"""
from __future__ import annotations

import collections
import threading
from enum import Enum

from app.models import LogEntry


class CompressionMode(str, Enum):
    """Advisory compression posture the analyzer recommends from observed churn.

    A ``str`` enum so each member is JSON-native (``CompressionMode.MAX == "max"``),
    which keeps the snapshot directly serializable without a custom encoder.
    """

    FAST = "fast"  # prioritize speed / random access (smaller intervals)
    BALANCED = "balanced"
    MAX = "max"  # prioritize compression (larger intervals)


class PatternAnalyzer:
    """Read-only sliding-window churn observer that *recommends* encoder settings.

    Accumulates per-step field-churn fractions in a bounded deque and turns the
    windowed mean into a keyframe-interval + compression-mode recommendation. It holds
    no encoder reference and mutates nothing outside itself — observing a batch can
    never change how that batch was (or will be) compressed.
    """

    def __init__(
        self,
        *,
        window: int = 200,
        current_interval: int = 100,
        min_interval: int = 10,
        max_interval: int = 500,
        mode: CompressionMode = CompressionMode.BALANCED,
    ) -> None:
        """Configure the window and the recommendation bounds.

        ``window`` caps the deque (oldest step-churns fall off the front).
        ``current_interval`` / ``mode`` are the engine's *configured* values, reported
        verbatim in the snapshot so the dashboard can show "current vs recommended".
        ``min_interval`` / ``max_interval`` bracket the recommended interval.
        """
        self._churns: collections.deque[float] = collections.deque(maxlen=window)
        self._current_interval = current_interval
        self._min_interval = min_interval
        self._max_interval = max_interval
        self._mode = mode
        self._lock = threading.Lock()

    def observe(self, entries: list[LogEntry]) -> None:
        """Record the per-step churn of each consecutive pair in ``entries``.

        For every adjacent pair ``(prev, cur)`` the churned-key count (added OR removed
        OR value-changed, by key-set membership — mirroring :func:`app.codec.diff_entries`)
        is divided by the size of the key union and appended to the bounded window. An
        empty or single-entry batch contributes nothing (there are no pairs). Read-only:
        this inspects the entries, it does not modify them or any encoder state.
        """
        if not entries or len(entries) < 2:
            return
        steps: list[float] = []
        for prev, cur in zip(entries, entries[1:]):
            prev_keys = set(prev)
            cur_keys = set(cur)
            union = prev_keys | cur_keys
            if not union:
                # Two empty entries: no fields at all ⇒ zero churn for this step.
                steps.append(0.0)
                continue
            # Churned = added/removed (symmetric difference) + changed-in-both.
            changed = sum(1 for k in (prev_keys & cur_keys) if prev[k] != cur[k])
            churned = len(prev_keys ^ cur_keys) + changed
            steps.append(churned / len(union))
        with self._lock:
            self._churns.extend(steps)

    def observed_churn(self) -> float:
        """Mean of the windowed step-churns (``round(., 4)``); ``0.0`` when empty."""
        with self._lock:
            if not self._churns:
                return 0.0
            return round(sum(self._churns) / len(self._churns), 4)

    def recommended_keyframe_interval(self) -> int:
        """Recommend a keyframe interval — monotonically NON-INCREASING in churn.

        Linear interpolation ``min + (max - min) * (1 - churn)`` clamped to
        ``[min_interval, max_interval]``: churn ``0`` ⇒ ``max_interval`` (low churn,
        store fewer keyframes), churn ``1`` ⇒ ``min_interval`` (high churn, more
        keyframes for random access). With no observations yet there is nothing to
        recommend, so the configured ``current_interval`` is returned unchanged.
        """
        with self._lock:
            if not self._churns:
                return self._current_interval
            churn = sum(self._churns) / len(self._churns)
        span = self._max_interval - self._min_interval
        value = round(self._min_interval + span * (1.0 - churn))
        return max(self._min_interval, min(self._max_interval, value))

    def recommended_mode(self) -> CompressionMode:
        """Recommend a mode: low churn ⇒ ``MAX``, high ⇒ ``FAST``, middle ⇒ ``BALANCED``.

        Thresholds on the windowed mean churn (``< 0.2`` ⇒ ``MAX``, ``> 0.6`` ⇒
        ``FAST``, else ``BALANCED``). With no observations the configured ``mode`` is
        returned (no recommendation to make).
        """
        with self._lock:
            if not self._churns:
                return self._mode
            churn = sum(self._churns) / len(self._churns)
        if churn < 0.2:
            return CompressionMode.MAX
        if churn > 0.6:
            return CompressionMode.FAST
        return CompressionMode.BALANCED

    def snapshot(self) -> dict:
        """Return the full advisory view as a JSON-native dict.

        Carries both the engine's configured values (``current_keyframe_interval`` /
        ``mode``) and the analyzer's suggestions (``recommended_*``), plus the window
        size and current sample count so the dashboard can show how settled the
        recommendation is. Every value is JSON-native (the modes are ``str`` enums).
        """
        return {
            "observed_churn": self.observed_churn(),
            "recommended_keyframe_interval": self.recommended_keyframe_interval(),
            "current_keyframe_interval": self._current_interval,
            "recommended_mode": self.recommended_mode().value,
            "mode": self._mode.value,
            "window": self._churns.maxlen,
            "samples": len(self._churns),
        }

    def reset(self) -> None:
        """Clear the observed window (configuration is kept). Thread-safe."""
        with self._lock:
            self._churns.clear()
