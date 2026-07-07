"""Error-cascade detector: ordered chains of errors hopping across services.

Failures propagate: the database pool saturates, the API starts throwing, the
web tier answers 5xx — one incident, three log streams. This detector rebuilds
that story from the window's ERROR-level events. Recent errors (the last
``2 * cascade_window_seconds``) are sorted by time and gap-clustered: two
consecutive errors belong to the same cluster while their gap stays within
``cascade_window_seconds``. A cluster spanning at least two distinct sources is
a cascade candidate and emits exactly ONE correlation: event_a is the chain's
root (earliest error), event_b its leaf (the latest error from a source other
than the root's). Candidates only emit while the leaf is fresh — within
:data:`FRESHNESS_SECONDS` of the cycle clock — so a cascade (re-)emits only
while cross-service propagation is actively observable. The 2-window lookback
admits errors up to 20 s stale, and a same-source error storm can keep a
cluster's newest error fresh while its leaf ages, so anchoring on anything but
the leaf (the emitted event_b, which the pair's detection latency is measured
against) would let that latency breach the 5 s contract.

Scoring: strength = 0.5 * (1 - dt/window) + 0.5 * min(1, distinct_sources/3) —
half proximity decay over the root->leaf span, half chain breadth. Confidence
starts at 0.4 and earns +0.3 when root and leaf share a correlation_id, +0.2
when they share a user_id, and +0.1 when the root->leaf direction matches a
known root-cause edge (:data:`ROOT_CAUSE_EDGES` — upstream infrastructure
failing into a user-facing tier).
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

#: Hard per-cycle emission bound: one pathological error storm can never flood
#: the downstream accumulators/Redis flush.
MAX_EMISSIONS_PER_TICK = 20

#: Known root-cause propagation directions: an upstream infrastructure failure
#: (key) cascading into the user-facing tiers it serves (values). A root->leaf
#: match earns the +0.1 confidence bonus.
ROOT_CAUSE_EDGES: dict[SourceType, frozenset[SourceType]] = {
    SourceType.DATABASE: frozenset({SourceType.WEB, SourceType.API_SERVICE}),
    SourceType.PAYMENT: frozenset({SourceType.WEB, SourceType.API_SERVICE}),
    SourceType.INVENTORY: frozenset({SourceType.API_SERVICE, SourceType.WEB}),
}

#: Levels that mark an event as a failure. Parsers normalize everything to
#: INFO/WARN/ERROR, but raw FATAL/CRITICAL are accepted defensively so a
#: hand-built or future event shape still chains.
_ERROR_LEVELS = frozenset({"ERROR", "FATAL", "CRITICAL"})

#: Breadth term denominator: a 3-source chain already counts as maximal spread.
_BREADTH_SCALE = 3.0

#: The details payload lists at most this many chain hops (chain_length still
#: reports the full count).
_CHAIN_DETAIL_MAX = 10


class CascadeDetector:
    """Gap-clusters recent cross-source errors into root->leaf cascade chains."""

    name = "error_cascade"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._dedupe = DedupeCache()

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Emit one correlation per fresh multi-source error cluster."""
        window = float(self.settings.cascade_window_seconds)
        if window <= 0.0 or not ctx.window_events:
            return []

        # Recent errors only: anything older than two cascade windows is
        # history, not an unfolding incident.
        cutoff = ctx.now - 2.0 * window
        errors = sorted(
            (
                ev
                for ev in ctx.window_events
                if ev.timestamp >= cutoff and ev.level.upper() in _ERROR_LEVELS
            ),
            key=_event_ts,
        )
        if len(errors) < 2:
            return []

        found: list[Correlation] = []
        ttl = float(self.settings.dedup_ttl_seconds)
        for chain in _clusters(errors, window):
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                break
            distinct = {ev.source for ev in chain}
            if len(distinct) < 2:
                continue  # a single service erroring alone is not a cascade
            root = chain[0]
            # Leaf: the LATEST error that actually crossed a service boundary
            # (guaranteed to exist because the cluster spans >= 2 sources).
            leaf = next(ev for ev in reversed(chain) if ev.source is not root.source)
            # Freshness guard on the LEAF — the emitted event_b, whose
            # timestamp is what the pair's detection latency is measured
            # against (the root is never newer). Guarding on the chain's
            # newest error is not enough: a same-source error storm keeps the
            # cluster "fresh" while its newest CROSS-SOURCE error ages up to
            # 20 s inside the lookback. Anchoring here means a cascade only
            # (re-)emits while propagation is actively observable. Skipped
            # clusters are NOT marked seen — the next cross-source error
            # revives them.
            if ctx.now - leaf.timestamp > FRESHNESS_SECONDS:
                continue  # cross-service propagation no longer observable
            # Dedupe LAST, keyed by the chain's shape (root/leaf pair + root
            # error), so an evolving incident re-emits only once per TTL.
            key = pair_key(
                self.name, root.source.value, leaf.source.value, root.error_code or "-"
            )
            if not self._dedupe.seen(key, ctx.now, ttl):
                continue  # this cascade already emitted within the TTL
            dt = leaf.timestamp - root.timestamp
            confidence = 0.4
            if root.correlation_id is not None and root.correlation_id == leaf.correlation_id:
                confidence += 0.3  # same journey end to end — near-certain propagation
            if root.user_id is not None and root.user_id == leaf.user_id:
                confidence += 0.2
            if leaf.source in ROOT_CAUSE_EDGES.get(root.source, frozenset()):
                confidence += 0.1  # matches a known failure-propagation direction
            found.append(
                Correlation(
                    id=new_correlation_id(),
                    detected_at=ctx.now,
                    correlation_type=CorrelationType.CASCADE,
                    event_a=EventRef.from_event(root),
                    event_b=EventRef.from_event(leaf),
                    strength=clamp01(
                        0.5 * (1.0 - dt / window)
                        + 0.5 * min(1.0, len(distinct) / _BREADTH_SCALE)
                    ),
                    confidence=clamp01(confidence),
                    details={
                        "chain": [
                            {
                                "source": ev.source.value,
                                "service": ev.service,
                                "error_code": ev.error_code,
                                "ts": ev.timestamp,
                            }
                            for ev in chain[:_CHAIN_DETAIL_MAX]
                        ],
                        "chain_length": len(chain),
                        "distinct_services": len(distinct),
                        "span_seconds": round(dt, 3),
                        "root_error": root.error_code,
                    },
                )
            )
        return found


def _clusters(errors: list[LogEvent], max_gap: float) -> list[list[LogEvent]]:
    """Split time-sorted errors wherever consecutive gaps exceed ``max_gap``."""
    clusters: list[list[LogEvent]] = []
    current = [errors[0]]
    for prev, ev in zip(errors, errors[1:]):
        if ev.timestamp - prev.timestamp <= max_gap:
            current.append(ev)
        else:
            clusters.append(current)
            current = [ev]
    clusters.append(current)
    return clusters


def _event_ts(ev: LogEvent) -> float:
    """Sort key: an event's own timestamp."""
    return ev.timestamp
