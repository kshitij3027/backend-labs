"""Base-rate anomaly amplification for the RCA Analysis Engine (C7, feature area D).

The :class:`AnomalyAmplifier` is the **base-rate correction** stage of
``RCAAnalyzer.analyze``. Its whole job is to stop *common, benign* events from being
amplified into false-positive root causes: a routine ``(database, INFO)`` that fires in
every incident must score LOW, while a genuinely rare ``(payment, CRITICAL)`` must score
HIGH. It turns a batch of :class:`~src.models.LogEvent` into a per-event
``anomaly_score in [0, 1]`` that the multi-hypothesis tracker (C7) then uses to seed its
personalized-PageRank restart vector.

An event's **type key** is the coarse, robust pair ``(service, level)``. The amplifier
keeps a small running **baseline** over those types — total occurrences, per-incident
counts (for a mean/variance), and how many past incidents contained each type — updated
by :meth:`observe`. Scoring blends three research-backed signals per type, plus a light
severity prior:

* **surprise** ``= -log2(p_type)`` with a Laplace-smoothed
  ``p_type = (baseline_count[type] + 1) / (baseline_total + K)`` (``K`` = the known-type
  vocabulary size). An unseen type gets the smallest ``p`` and hence the highest
  surprise, and there is never a division by zero.
* **z-score** of the type's in-incident count against its baseline per-incident mean /
  std, capturing a sudden *burst* of an otherwise-quiet type (std ``== 0`` is guarded to
  contribute nothing).
* **Ochiai spectrum lift** ``ef / sqrt((ef + nf)(ef + ep))`` treating the current
  incident as the single *failing* window and history as the *passing* population: with
  ``ef = 1`` (the type is present in the failing window) and ``nf = 0``, this collapses to
  ``1 / sqrt(1 + ep)`` where ``ep`` is the number of past incidents containing the type.
  A type that fired in *every* historical window is driven toward zero (suppressed),
  while a never-before-seen type scores ``1.0``.

The surprise and z-score are min-max normalized *within the incident* so they are
comparable; Ochiai and severity are already in ``[0, 1]``. The weighted blend is itself
in ``[0, 1]`` (weights sum to 1), and the KEY property holds by construction: **rare
types score high, high-base-rate types score low**.

When there is **no history yet** (the first incident an analyzer ever sees), surprise,
z-score and Ochiai carry no information, so the amplifier degrades gracefully to a
``local-surprise + severity`` fallback — an event that is rare *within its own incident*
and/or severe still stands out — rather than returning a meaningless uniform vector.

The amplifier is **pure and deterministic** given its accumulated baseline: no network,
no globals, no wall-clock reads. Ordering matters at the call site — ``analyze`` scores
an incident *before* it :meth:`observe`\\s it, so an incident is always graded against
prior history only and can never trivially explain itself.
"""

from __future__ import annotations

import math
from collections import Counter

from src.analysis.timeline import _derive_event_id
from src.config import Settings
from src.models import LogEvent, LogLevel

__all__ = ["AnomalyAmplifier"]

#: Type key = ``(service, level_value)``.
TypeKey = tuple[str, str]

#: Blend weights for the history-informed score (sum to 1.0). The two base-rate signals
#: (surprise + Ochiai) dominate so the correction is driven by how *rare* a type is, not
#: by severity alone.
_W_SURPRISE: float = 0.35
_W_OCHIAI: float = 0.30
_W_ZSCORE: float = 0.15
_W_SEVERITY: float = 0.20

#: Blend weights for the empty-history fallback (sum to 1.0): local (within-incident)
#: rarity and severity, since no baseline signal is available yet.
_FB_W_LOCAL: float = 0.5
_FB_W_SEVERITY: float = 0.5

#: Range below which a min-max normalization is treated as degenerate (all-equal).
_EPS: float = 1e-12


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into the inclusive ``[low, high]`` range."""
    return max(low, min(high, value))


def _min_max(values: dict[TypeKey, float]) -> dict[TypeKey, float]:
    """Min-max scale a ``type -> value`` map into ``[0, 1]``.

    A degenerate (all-equal) map carries no discriminating information and is mapped to
    all-zeros, so the component simply drops out of the weighted blend.
    """
    if not values:
        return {}
    lo = min(values.values())
    hi = max(values.values())
    if hi - lo <= _EPS:
        return {key: 0.0 for key in values}
    span = hi - lo
    return {key: (value - lo) / span for key, value in values.items()}


class AnomalyAmplifier:
    """Score events by base-rate-corrected anomaly so common events aren't amplified."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        #: Number of incidents (batches) folded into the baseline so far.
        self._incident_count: int = 0
        #: Total event occurrences across all observed incidents (``baseline_total``).
        self._total: int = 0
        #: type -> total occurrences across history (``baseline_count``).
        self._type_total: Counter[TypeKey] = Counter()
        #: type -> sum of squared per-incident counts (for the baseline variance).
        self._type_sumsq: dict[TypeKey, float] = {}
        #: type -> number of incidents containing it at least once (Ochiai ``ep``).
        self._type_incidents: Counter[TypeKey] = Counter()
        #: Every type ever seen (baseline vocabulary size ``K`` for Laplace smoothing).
        self._known_types: set[TypeKey] = set()

    # --- Baseline maintenance ----------------------------------------------------

    def observe(self, events: list[LogEvent]) -> None:
        """Fold one incident's type counts into the running baseline.

        Called once per incident (with that incident's events) so the baseline learns
        over time. An empty batch is ignored — it is not a meaningful "incident" for the
        per-incident mean/variance and would only dilute the statistics.
        """
        if not events:
            return
        counts: Counter[TypeKey] = Counter(
            (event.service, event.level.value) for event in events
        )
        self._incident_count += 1
        self._total += sum(counts.values())
        for type_key, count in counts.items():
            self._type_total[type_key] += count
            self._type_sumsq[type_key] = self._type_sumsq.get(type_key, 0.0) + count * count
            self._type_incidents[type_key] += 1
            self._known_types.add(type_key)

    # --- Per-type signals --------------------------------------------------------

    def _surprise(self, type_key: TypeKey) -> float:
        """Laplace-smoothed surprise ``-log2(p_type)`` (higher == rarer in history)."""
        vocabulary = max(len(self._known_types | {type_key}), 1)
        p_type = (self._type_total.get(type_key, 0) + 1) / (self._total + vocabulary)
        return -math.log2(p_type)

    def _zscore(self, type_key: TypeKey, incident_count: int) -> float:
        """Z-score of the type's in-incident count vs. its baseline per-incident stats.

        Returns ``0.0`` when there is no history or the baseline std is ``0`` (guarded),
        so an unseen or perfectly-steady type leans on the other signals instead.
        """
        n = self._incident_count
        if n <= 0:
            return 0.0
        mean = self._type_total.get(type_key, 0) / n
        variance = self._type_sumsq.get(type_key, 0.0) / n - mean * mean
        std = math.sqrt(variance) if variance > 0.0 else 0.0
        if std <= 0.0:
            return 0.0
        return (incident_count - mean) / std

    def _ochiai(self, type_key: TypeKey) -> float:
        """Ochiai spectrum lift ``ef / sqrt((ef + nf)(ef + ep))`` for a present type.

        The current incident is the single failing window, so ``ef = 1``, ``nf = 0`` and
        ``ep`` is the number of historical incidents containing the type, collapsing to
        ``1 / sqrt(1 + ep)``. A ubiquitous type (large ``ep``) is suppressed toward 0; a
        never-seen type (``ep = 0``) scores ``1.0``.
        """
        ep = self._type_incidents.get(type_key, 0)
        ef, nf = 1.0, 0.0
        denom = math.sqrt((ef + nf) * (ef + ep))
        return ef / denom if denom > 0.0 else 0.0

    def _severity(self, level_value: str) -> float:
        """Severity prior in ``[0, 1]`` (CRITICAL -> 1.0 ... INFO -> 0.0).

        Derived from the same ``score_*`` knobs the confidence scorer uses, normalized by
        ``score_critical`` so the scale is config-consistent and independent of the raw
        magnitudes.
        """
        s = self.settings
        raw = {
            LogLevel.CRITICAL.value: s.score_critical,
            LogLevel.ERROR.value: s.score_error,
            LogLevel.WARNING.value: s.score_warning,
            LogLevel.INFO.value: 0.0,
        }.get(level_value, 0.0)
        denom = s.score_critical if s.score_critical > 0 else 1.0
        return _clamp(raw / denom, 0.0, 1.0)

    # --- Scoring -----------------------------------------------------------------

    def score(self, events: list[LogEvent]) -> dict[str, float]:
        """Return ``{event_id: anomaly_score in [0, 1]}`` for one incident's events.

        Blends surprise, z-score, Ochiai lift and a severity prior against the *prior*
        baseline (the caller scores before it :meth:`observe`\\s, so the current incident
        is excluded). All events of a given ``(service, level)`` type share the type's
        score. With no history yet, falls back to within-incident rarity + severity.
        Empty input yields an empty dict.
        """
        if not events:
            return {}

        # Resolve ids exactly as the rest of the pipeline does, and tally the incident's
        # per-type counts. Every event's type is remembered so its score can be looked up.
        incident_counts: Counter[TypeKey] = Counter()
        event_types: list[tuple[str, TypeKey]] = []
        for index, event in enumerate(events):
            event_id = event.event_id or _derive_event_id(index, event)
            type_key: TypeKey = (event.service, event.level.value)
            incident_counts[type_key] += 1
            event_types.append((event_id, type_key))

        present_types = list(incident_counts)
        has_history = self._incident_count > 0 and self._total > 0

        if has_history:
            combined = self._history_scores(present_types, incident_counts)
        else:
            combined = self._fallback_scores(present_types, incident_counts)

        return {
            event_id: _clamp(combined[type_key], 0.0, 1.0)
            for event_id, type_key in event_types
        }

    def _history_scores(
        self, present_types: list[TypeKey], incident_counts: Counter[TypeKey]
    ) -> dict[TypeKey, float]:
        """History-informed per-type blend (surprise + Ochiai + z-score + severity)."""
        surprise_n = _min_max({t: self._surprise(t) for t in present_types})
        # Only a positive z (a burst above baseline) is anomalous; a below-baseline count
        # is not evidence of a root cause.
        zscore_n = _min_max(
            {t: max(self._zscore(t, incident_counts[t]), 0.0) for t in present_types}
        )
        return {
            t: (
                _W_SURPRISE * surprise_n[t]
                + _W_OCHIAI * self._ochiai(t)
                + _W_ZSCORE * zscore_n[t]
                + _W_SEVERITY * self._severity(t[1])
            )
            for t in present_types
        }

    def _fallback_scores(
        self, present_types: list[TypeKey], incident_counts: Counter[TypeKey]
    ) -> dict[TypeKey, float]:
        """Empty-history fallback: within-incident rarity + severity only.

        With no baseline, a type's ``-log2`` of its *own* in-incident frequency stands in
        for surprise, so a singleton stands out from a repeated routine type; severity
        breaks the remaining ties.
        """
        total = sum(incident_counts.values())
        local_surprise = {
            t: -math.log2(incident_counts[t] / total) for t in present_types
        }
        local_n = _min_max(local_surprise)
        return {
            t: (_FB_W_LOCAL * local_n[t] + _FB_W_SEVERITY * self._severity(t[1]))
            for t in present_types
        }
