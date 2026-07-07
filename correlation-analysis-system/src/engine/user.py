"""User-based detector: one user active across DIFFERENT checkout journeys.

The session detector links the hops of one journey; this detector links the
*journeys* of one user. Window events are grouped by ``user_id`` (background
noise has none and is ignored). A user becomes a candidate once the window
holds at least two distinct journeys (non-None ``correlation_id``s) touching at
least two distinct sources — the signal being "the same person keeps coming
back and exercising several services", which no single journey shows.

Scoring mirrors the session detector's coverage idea: coverage =
distinct_sources / 5, strength = coverage, confidence = 0.5 + 0.3 * coverage —
a floor lower than the session detector's because cross-journey linkage is
inherently weaker evidence than shared-id hops. One emission per user per
dedupe TTL, and only when anchored to fresh activity: the user's newest event
must be within :data:`FRESHNESS_SECONDS` of the cycle clock, so old events
lingering in the window never re-emit with a current ``detected_at`` after the
TTL lapses.
"""

from __future__ import annotations

from src.config import Settings
from src.engine.base import (
    DedupeCache,
    DetectionContext,
    FRESHNESS_SECONDS,
    clamp01,
    new_correlation_id,
    pair_key,
)
from src.models import Correlation, CorrelationType, EventRef, LogEvent, SourceType

#: Hard per-cycle emission bound (mirrors the cascade detector's cap).
MAX_EMISSIONS_PER_TICK = 20

#: Coverage denominator: a user can touch at most all five sources.
_TOTAL_SOURCES = len(SourceType)

#: Minimum distinct journeys before "same user, different flows" means anything.
_MIN_JOURNEYS = 2

#: Minimum distinct sources before the user's activity is a cross-source signal.
_MIN_DISTINCT_SOURCES = 2


class UserDetector:
    """Emits one correlation per multi-journey user, anchored to fresh activity."""

    name = "user_based"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._dedupe = DedupeCache()

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Scan the window's per-user activity; emit each qualifying user once."""
        by_user: dict[str, list[LogEvent]] = {}
        for ev in ctx.window_events:
            if ev.user_id is not None:
                by_user.setdefault(ev.user_id, []).append(ev)

        found: list[Correlation] = []
        ttl = float(self.settings.dedup_ttl_seconds)
        for user_id, events in by_user.items():
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                break
            journeys = {
                ev.correlation_id for ev in events if ev.correlation_id is not None
            }
            if len(journeys) < _MIN_JOURNEYS:
                continue  # a single journey is the session detector's business
            distinct = {ev.source for ev in events}
            if len(distinct) < _MIN_DISTINCT_SOURCES:
                continue  # single-source activity carries no cross-source signal
            # Freshness guard: only emit while anchored to CURRENT activity.
            # After the dedupe TTL lapses, a user still qualifying purely from
            # old events lingering in the 30 s window would otherwise re-emit
            # with detected_at = now against ~TTL-old events — an apparent
            # detection latency of dedupe TTL + tick, far past the 5 s budget.
            if ctx.now - max(ev.timestamp for ev in events) > FRESHNESS_SECONDS:
                continue  # stale remnants only — wait for fresh activity
            # Dedupe LAST: a user gated out above (single journey, single
            # source, or stale activity) must not get marked as seen, or they
            # could never emit once a second journey / fresh event shows up.
            if not self._dedupe.seen(pair_key(self.name, user_id), ctx.now, ttl):
                continue  # this user already emitted within the TTL
            events.sort(key=_event_ts)
            first, last = events[0], events[-1]
            coverage = len(distinct) / _TOTAL_SOURCES
            found.append(
                Correlation(
                    id=new_correlation_id(),
                    detected_at=ctx.now,
                    correlation_type=CorrelationType.USER,
                    event_a=EventRef.from_event(first),
                    event_b=EventRef.from_event(last),
                    strength=clamp01(coverage),
                    confidence=clamp01(0.5 + 0.3 * coverage),
                    details={
                        "user_id": user_id,
                        "distinct_sources": len(distinct),
                        "journey_count": len(journeys),
                        "span_seconds": round(last.timestamp - first.timestamp, 3),
                    },
                )
            )
        return found


def _event_ts(ev: LogEvent) -> float:
    """Sort key: an event's own timestamp."""
    return ev.timestamp
