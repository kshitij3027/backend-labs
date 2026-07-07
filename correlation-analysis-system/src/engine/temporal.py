"""Temporal-proximity detector: "these two source streams co-occur right now".

For every event parsed since the last cycle, find the nearest-in-time window
event of each OTHER source; a source pair whose nearest gap is within
``settings.window_seconds`` is temporally proximate. Per cycle the detector
keeps only the tightest candidate per source pair, and a TTL dedupe
(``settings.dedup_ttl_seconds``) re-emits each pair at most once per TTL —
"web and database are moving together" is one finding, not one per event.

Scoring: strength decays linearly with the gap (``1 - dt/window``); confidence
scales with per-cycle support (how many nearest-neighbour candidates the pair
produced this cycle, ``support/10``), clamped into [0.1, 0.9]. Same-source
pairs are never emitted.
"""

from __future__ import annotations

from bisect import bisect_left

from src.config import Settings
from src.engine.base import (
    DedupeCache,
    DetectionContext,
    clamp01,
    new_correlation_id,
    pair_key,
)
from src.models import Correlation, CorrelationType, EventRef, LogEvent, SourceType

#: Hard per-cycle emission bound. With 5 sources there are only 10 unordered
#: pairs, but the cap guarantees one pathological cycle can never flood the
#: downstream accumulators/Redis flush.
MAX_EMISSIONS_PER_TICK = 50

#: Confidence = clamp(support / _SUPPORT_SCALE) into [floor, ceil]: 1 candidate
#: pair scores 0.1, 9+ candidates saturate at 0.9 — co-occurrence backed by many
#: event pairs is trusted more than a single coincidence.
_SUPPORT_SCALE = 10.0
_CONFIDENCE_FLOOR = 0.1
_CONFIDENCE_CEIL = 0.9


class TemporalDetector:
    """Nearest-neighbour cross-source proximity over the sliding window."""

    name = "temporal"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._dedupe = DedupeCache()

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Emit at most one proximity correlation per source pair this cycle."""
        window = float(self.settings.window_seconds)
        if window <= 0.0 or not ctx.new_events or not ctx.window_events:
            return []

        # Per-source events + sorted timestamp arrays, built once per cycle.
        # The buffer arrives roughly time-ordered, so these sorts are ~O(n).
        per_source: dict[SourceType, list[LogEvent]] = {}
        for ev in ctx.window_events:
            per_source.setdefault(ev.source, []).append(ev)
        stamps: dict[SourceType, list[float]] = {}
        for source, events in per_source.items():
            events.sort(key=_event_ts)
            stamps[source] = [ev.timestamp for ev in events]

        # For each new event x each OTHER source: the nearest window event by
        # |dt|. Keep only the tightest candidate per unordered source pair,
        # counting every candidate toward that pair's per-cycle support.
        best: dict[tuple[str, str], tuple[float, LogEvent, LogEvent]] = {}
        support: dict[tuple[str, str], int] = {}
        for ev in ctx.new_events:
            for source, source_stamps in stamps.items():
                if source is ev.source:
                    continue  # same-source pairs are never emitted
                other, dt = _nearest(source_stamps, per_source[source], ev.timestamp)
                if other is None or dt > window:
                    continue
                pair = _pair(ev.source, source)
                support[pair] = support.get(pair, 0) + 1
                held = best.get(pair)
                if held is None or dt < held[0]:
                    best[pair] = (dt, ev, other)

        found: list[Correlation] = []
        ttl = float(self.settings.dedup_ttl_seconds)
        for pair, (dt, ev, other) in best.items():
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                break
            if not self._dedupe.seen(pair_key(self.name, *pair), ctx.now, ttl):
                continue  # this source pair already emitted within the TTL
            first, second = (ev, other) if ev.timestamp <= other.timestamp else (other, ev)
            found.append(
                Correlation(
                    id=new_correlation_id(),
                    detected_at=ctx.now,
                    correlation_type=CorrelationType.TEMPORAL,
                    event_a=EventRef.from_event(first),
                    event_b=EventRef.from_event(second),
                    strength=clamp01(1.0 - dt / window),
                    confidence=clamp01(
                        min(
                            _CONFIDENCE_CEIL,
                            max(_CONFIDENCE_FLOOR, support[pair] / _SUPPORT_SCALE),
                        )
                    ),
                    details={"dt_seconds": round(dt, 3), "support": support[pair]},
                )
            )
        return found


def _event_ts(ev: LogEvent) -> float:
    """Sort key: an event's own timestamp."""
    return ev.timestamp


def _pair(a: SourceType, b: SourceType) -> tuple[str, str]:
    """The unordered source pair as a sorted value tuple ((a,b) == (b,a))."""
    return (a.value, b.value) if a.value <= b.value else (b.value, a.value)


def _nearest(
    stamps: list[float], events: list[LogEvent], ts: float
) -> tuple[LogEvent | None, float]:
    """The event whose timestamp is closest to ``ts`` (bisect on sorted stamps)."""
    idx = bisect_left(stamps, ts)
    nearest: LogEvent | None = None
    nearest_dt = float("inf")
    if idx < len(stamps):
        nearest, nearest_dt = events[idx], abs(stamps[idx] - ts)
    if idx > 0:
        dt_prev = abs(ts - stamps[idx - 1])
        if dt_prev < nearest_dt:
            nearest, nearest_dt = events[idx - 1], dt_prev
    return nearest, nearest_dt
