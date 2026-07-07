"""Session detector: one finished checkout journey = one correlation.

Groups window events by their shared ``correlation_id`` (background noise has
none and is ignored). A journey emits exactly once, and only when it is
*finished*: journeys' hops span at most ~4 s by construction (see
:mod:`src.generators`), so a journey whose newest hop is older than
:data:`QUIESCENCE_SECONDS` has stopped producing. Coverage — how many of the 5
sources the journey touched — drives both scores: strength = coverage,
confidence = 0.7 + 0.3 * coverage.
"""

from __future__ import annotations

from src.config import Settings
from src.engine.base import (
    DedupeCache,
    DetectionContext,
    clamp01,
    new_correlation_id,
    pair_key,
)
from src.models import Correlation, CorrelationType, EventRef, LogEvent, SourceType

#: A journey is "finished" once its newest hop is at least this old. Hop gaps
#: stay under ~1.2 s (generator constants), so 2.5 s of silence means no
#: further hop is coming — while keeping detection latency well inside the 5 s
#: end-to-end budget.
QUIESCENCE_SECONDS = 2.5

#: One emission per journey, ever: this TTL only has to outlive the sliding
#: window (30 s) — once the journey's events age out of the buffer, the group
#: cannot re-form anyway.
_DEDUPE_TTL_SECONDS = 60.0

#: Coverage denominator: a journey can touch at most all five sources.
_TOTAL_SOURCES = len(SourceType)

#: Minimum distinct sources before a journey counts as a cross-source signal.
_MIN_DISTINCT_SOURCES = 2


class SessionDetector:
    """Emits one correlation per finished multi-source journey."""

    name = "session_based"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._dedupe = DedupeCache()

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Scan the window's journeys; emit each finished one exactly once."""
        journeys: dict[str, list[LogEvent]] = {}
        for ev in ctx.window_events:
            if ev.correlation_id is not None:
                journeys.setdefault(ev.correlation_id, []).append(ev)

        found: list[Correlation] = []
        for journey_id, hops in journeys.items():
            hops.sort(key=_event_ts)
            if ctx.now - hops[-1].timestamp < QUIESCENCE_SECONDS:
                continue  # journey may still be producing hops — wait
            distinct = {hop.source for hop in hops}
            if len(distinct) < _MIN_DISTINCT_SOURCES:
                continue  # single-source groups carry no cross-source signal
            # Dedupe LAST: a journey skipped by the gates above must not get
            # marked as seen, or it could never emit once it finishes.
            if not self._dedupe.seen(
                pair_key(self.name, journey_id), ctx.now, _DEDUPE_TTL_SECONDS
            ):
                continue  # this journey already emitted
            first, last = hops[0], hops[-1]
            coverage = len(distinct) / _TOTAL_SOURCES
            found.append(
                Correlation(
                    id=new_correlation_id(),
                    detected_at=ctx.now,
                    correlation_type=CorrelationType.SESSION,
                    event_a=EventRef.from_event(first),
                    event_b=EventRef.from_event(last),
                    strength=clamp01(coverage),
                    confidence=clamp01(0.7 + 0.3 * coverage),
                    details={
                        "hops": [hop.source.value for hop in hops],
                        "distinct_sources": len(distinct),
                        "span_seconds": round(last.timestamp - first.timestamp, 3),
                        "user_id": next((h.user_id for h in hops if h.user_id), None),
                    },
                )
            )
        return found


def _event_ts(ev: LogEvent) -> float:
    """Sort key: an event's own timestamp."""
    return ev.timestamp
