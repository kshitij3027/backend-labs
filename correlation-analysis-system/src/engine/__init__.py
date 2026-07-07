"""The correlation engine: runs every registered detector once per detection
cycle and owns all detection-side accumulators — the recent-correlations deque,
lifetime counters, the 10-minute timeline, the 5x5 source matrix — plus the
Redis persistence hand-off.

C4 registered the temporal + session detectors; C5 added cascade + user plus
the AlertManager hand-off; C6 added the metric detector (one BH-FDR
significance pass per cycle); C7 wires the PatternLearner in. The
engine, like the rest of the pipeline, is single-threaded by design: detect()
runs synchronously inside the pipeline loop and the API reads the accumulators
between ticks on the same event loop — no locking anywhere.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any

import numpy as np

from src.aggregation import MetricAggregator
from src.config import Settings
from src.engine.base import DetectionContext, Detector, clamp01
from src.engine.cascade import CascadeDetector
from src.engine.metric import MetricDetector
from src.engine.session import SessionDetector
from src.engine.temporal import TemporalDetector
from src.engine.user import UserDetector
from src.models import Correlation, CorrelationType, LogEvent, SourceType
from src.patterns import PatternLearner
from src.store import RedisStore

__all__ = ["CorrelationEngine"]

logger = logging.getLogger(__name__)

#: In-memory retention of detected correlations (mirrors the Redis list cap).
RECENT_MAX = 2000

#: Timeline granularity: 60 buckets x 10 s = the last ~10 minutes of activity.
_TIMELINE_BUCKET_SECONDS = 10
_TIMELINE_BUCKETS = 60

#: Source-matrix EMA smoothing (higher = more reactive to the newest strength).
_MATRIX_EMA_ALPHA = 0.2


class CorrelationEngine:
    """Detector orchestration + detection-side state (see module docstring)."""

    def __init__(
        self,
        settings: Settings,
        aggregator: MetricAggregator | None,
        store: RedisStore | None = None,
        patterns: PatternLearner | None = None,
        alerts: Any | None = None,
    ) -> None:
        self.settings = settings
        self.aggregator = aggregator
        #: Optional Redis mirror; None (or a dead Redis) degrades to memory-only.
        self.store = store
        #: PatternLearner (C7): each cycle's findings are assessed against it
        #: BEFORE persistence (boost/anomaly/new flags), then recorded into it.
        self.patterns = patterns
        #: AlertManager (duck-typed on purpose: :mod:`src.alerts` imports
        #: :mod:`src.engine.base`, so naming the class here would import-cycle).
        self.alerts = alerts
        #: Detector registry, run in order each cycle. Each detector owns its own
        #: dedupe cache internally — no engine-level dedupe is needed.
        self.detectors: list[Detector] = [
            TemporalDetector(settings),
            SessionDetector(settings),
            CascadeDetector(settings),
            UserDetector(settings),
            MetricDetector(settings),
        ]

        # --- In-memory accumulators (the API reads these, never Redis) ---------
        #: Every detected correlation, oldest -> newest, bounded like the mirror.
        self.correlations: deque[Correlation] = deque(maxlen=RECENT_MAX)
        self.total = 0
        self.by_type: dict[str, int] = {}
        self.strength_sum = 0.0
        #: bucket_start -> {"count", "strength_sum", "by_type"}; pruned to the
        #: newest ``_TIMELINE_BUCKETS`` buckets.
        self._timeline: dict[int, dict[str, Any]] = {}
        #: 5x5 strength EMA per source pair, in canonical SourceType order, plus
        #: a touched mask so matrix() can report 0.0 for never-seen pairs.
        self._matrix_sources: list[str] = [source.value for source in SourceType]
        self._source_idx = {value: i for i, value in enumerate(self._matrix_sources)}
        size = len(self._matrix_sources)
        self._matrix = np.zeros((size, size))
        self._matrix_touched = np.zeros((size, size), dtype=bool)

    # --- Detection cycle ----------------------------------------------------------
    def detect(
        self, new_events: list[LogEvent], window_events: list[LogEvent], now: float
    ) -> list[Correlation]:
        """Run every registered detector once and fold the results into state.

        Each detector runs inside its own guard: one buggy detector logs and is
        skipped for the cycle, never killing the pipeline loop. Findings are
        then pattern-assessed (confidence boost, anomaly and new-pattern flags)
        BEFORE they are recorded, persisted, or alerted on, so every downstream
        consumer — Redis mirror, alert rules, API readers — sees the enriched
        versions. Results are accumulated in memory and mirrored to Redis
        (best-effort) in one go.
        """
        ctx = DetectionContext(
            now=now,
            new_events=new_events,
            window_events=window_events,
            aggregator=self.aggregator,
            patterns=self.patterns,
        )
        found: list[Correlation] = []
        for detector in self.detectors:
            try:
                found.extend(detector.detect(ctx))
            except Exception:  # noqa: BLE001 — a detector bug must not kill the loop
                logger.exception("detector %r failed; skipping it this cycle", detector.name)

        if found:
            # C7: pattern learning. Assess each finding against the baseline
            # BEFORE this observation (boost confidence by recurrence, flag >2σ
            # anomalies and brand-new strong patterns), then record the whole
            # batch — so a pattern's Nth detection is always judged against the
            # previous N-1.
            if self.patterns is not None:
                for corr in found:
                    assessment = self.patterns.assess(corr, now)
                    corr.confidence = clamp01(corr.confidence + assessment.boost)
                    # Count INCLUDING this observation: a re-detection carries >= 2.
                    corr.details["pattern_count"] = assessment.count + 1
                    if assessment.is_anomalous:
                        corr.details["anomaly"] = True
                    if assessment.is_new:
                        corr.details["new_pattern"] = True
                self.patterns.record(found, now)

            self._record(found)
            if self.store is not None:
                self.store.push_correlations(found)
                self.store.incr_stats(found)
                self.store.incr_minute_stats(found, now)
            # C5: the alert rules run over this cycle's fresh findings — the
            # manager owns thresholds + cooldowns, the store fans fired alerts
            # out (capped list + pub/sub channel).
            if self.alerts is not None:
                new_alerts = self.alerts.evaluate(found, now)
                if new_alerts and self.store is not None:
                    self.store.push_alerts(new_alerts)
        return found

    # --- Read side (API) ------------------------------------------------------------
    def stats(self, now: float | None = None) -> dict[str, Any]:
        """The spec-verbatim stats payload: total / types / avg_strength / recent_count.

        ``recent_count`` is the number of retained correlations detected in the
        last 60 seconds relative to ``now`` (wall clock when omitted — tests
        driving a simulated clock pass their own ``now``).
        """
        if self.total == 0:
            return {"total": 0, "types": {}, "avg_strength": 0.0, "recent_count": 0}
        if now is None:
            now = time.time()
        cutoff = now - 60.0
        recent_count = sum(1 for corr in self.correlations if corr.detected_at >= cutoff)
        return {
            "total": self.total,
            "types": dict(self.by_type),
            "avg_strength": round(self.strength_sum / self.total, 4),
            "recent_count": recent_count,
        }

    def recent(
        self,
        limit: int = 50,
        ctype: CorrelationType | None = None,
        min_strength: float = 0.0,
    ) -> list[Correlation]:
        """The newest retained correlations, newest first, optionally filtered."""
        if limit <= 0:
            return []
        out: list[Correlation] = []
        for corr in reversed(self.correlations):  # deque appends newest at the right
            if ctype is not None and corr.correlation_type is not ctype:
                continue
            if corr.strength < min_strength:
                continue
            out.append(corr)
            if len(out) >= limit:
                break
        return out

    def timeline(self, buckets: int = 60) -> list[dict[str, Any]]:
        """Per-10s detection activity, oldest bucket first (at most ``buckets``)."""
        if buckets <= 0:
            return []
        items = sorted(self._timeline.items())[-buckets:]
        return [
            {
                "t": bucket_start,
                "count": entry["count"],
                "avg_strength": round(entry["strength_sum"] / entry["count"], 4),
                "by_type": dict(entry["by_type"]),
            }
            for bucket_start, entry in items
        ]

    def matrix(self) -> dict[str, Any]:
        """The 5x5 source-pair strength matrix (symmetric; 0.0 where untouched)."""
        size = len(self._matrix_sources)
        cells = [
            [
                round(float(self._matrix[i, j]), 3) if self._matrix_touched[i, j] else 0.0
                for j in range(size)
            ]
            for i in range(size)
        ]
        return {"sources": list(self._matrix_sources), "cells": cells}

    # --- Accumulator updates -----------------------------------------------------
    def _record(self, found: list[Correlation]) -> None:
        """Fold a cycle's correlations into deque/counters/timeline/matrix."""
        for corr in found:
            self.correlations.append(corr)
            self.total += 1
            type_value = corr.correlation_type.value
            self.by_type[type_value] = self.by_type.get(type_value, 0) + 1
            self.strength_sum += corr.strength
            self._update_timeline(corr)
            self._update_matrix(corr)

    def _update_timeline(self, corr: Correlation) -> None:
        bucket = (
            int(corr.detected_at // _TIMELINE_BUCKET_SECONDS) * _TIMELINE_BUCKET_SECONDS
        )
        entry = self._timeline.get(bucket)
        if entry is None:
            entry = {"count": 0, "strength_sum": 0.0, "by_type": {}}
            self._timeline[bucket] = entry
            self._prune_timeline(bucket)
        entry["count"] += 1
        entry["strength_sum"] += corr.strength
        type_value = corr.correlation_type.value
        entry["by_type"][type_value] = entry["by_type"].get(type_value, 0) + 1

    def _prune_timeline(self, newest_bucket: int) -> None:
        """Keep only the newest ``_TIMELINE_BUCKETS`` buckets' worth of history."""
        cutoff = newest_bucket - (_TIMELINE_BUCKETS - 1) * _TIMELINE_BUCKET_SECONDS
        for bucket in [b for b in self._timeline if b < cutoff]:
            del self._timeline[bucket]

    def _update_matrix(self, corr: Correlation) -> None:
        ia = self._source_idx.get(corr.event_a.source.value)
        ib = self._source_idx.get(corr.event_b.source.value)
        if ia is None or ib is None:  # defensive: unknown source value
            return
        # The {(i,j),(j,i)} set collapses to one cell on the diagonal, so a
        # same-source correlation is folded once, not twice.
        for i, j in {(ia, ib), (ib, ia)}:
            if self._matrix_touched[i, j]:
                self._matrix[i, j] += _MATRIX_EMA_ALPHA * (corr.strength - self._matrix[i, j])
            else:
                # Seed the first observation directly — EMA-from-zero would
                # understate a brand-new pair by 5x.
                self._matrix[i, j] = corr.strength
                self._matrix_touched[i, j] = True
