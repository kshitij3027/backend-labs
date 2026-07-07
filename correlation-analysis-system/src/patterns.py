"""Pattern learning: recurrence baselines that boost confidence and flag anomalies.

A *pattern* is a correlation family plus its two normalized endpoints —
``(correlation_type, a, b)`` where the endpoints are the two source values for
event-pair correlations (temporal / session / user / cascade) and the two
series names (``details.metric_a`` / ``details.metric_b``) for metric_based
ones, sorted so direction can never split one pattern in two.

The :class:`PatternLearner` keeps every baseline (observation count, strength
sum, strength sum-of-squares, first/last seen) in a plain in-memory dict — the
per-cycle :meth:`PatternLearner.assess` hot path never touches Redis. The dict
is hydrated from Redis exactly once (lazily, on the first assess/record) via
:meth:`src.store.RedisStore.load_patterns` so learned history survives
restarts, and every :meth:`PatternLearner.record` mirrors its increments back
fire-and-forget via :meth:`src.store.RedisStore.record_patterns`. The process
is single-threaded, so local-update-after-write keeps the dict exactly
consistent with what Redis accumulates; a dead Redis degrades to session-local
learning (and a fresh process then starts from an empty baseline — zero boost,
never an exception).

Scoring (plan, scoring table): confidence boost ``min(0.15, 0.03·ln(1+count))``
computed against the count BEFORE the current observation; anomaly when an
established pattern (count >= 5) sees a strength more than 2σ from its learned
mean; ``is_new`` marks the first sighting of an already-strong (>= 0.8)
relationship.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from src.config import Settings
from src.models import Correlation, CorrelationType
from src.store import RedisStore

__all__ = [
    "PatternAssessment",
    "PatternLearner",
    "pattern_endpoints",
    "pattern_key",
]

logger = logging.getLogger(__name__)

#: Confidence-boost ceiling — recurrence can never add more than this.
BOOST_CAP = 0.15
#: Boost scale: boost = min(BOOST_CAP, BOOST_SCALE * ln(1 + prior_count)).
BOOST_SCALE = 0.03
#: Minimum prior observations before the anomaly test is trusted at all.
ANOMALY_MIN_COUNT = 5
#: Anomalous when |strength - learned mean| exceeds this many learned σ.
ANOMALY_SIGMA_MULTIPLIER = 2.0
#: σ floor for the anomaly threshold: a σ≈0 baseline (identical strengths so
#: far) must flag genuine deviations without tripping on float rounding noise.
_SIGMA_FLOOR = 1e-9
#: First sighting of a pattern at or above this strength is flagged ``is_new``.
NEW_PATTERN_MIN_STRENGTH = 0.8

#: (correlation_type value, endpoint_a, endpoint_b) — one pattern's identity.
PatternKey = tuple[str, str, str]


def pattern_endpoints(corr: Correlation) -> tuple[str, str]:
    """The correlation's two normalized endpoint names, sorted.

    metric_based correlations are identified by the series they relate
    (``details.metric_a`` / ``details.metric_b``); every event-pair family is
    identified by its two source values. Sorting makes A→B and B→A the same
    pattern. A metric correlation missing its series names (fabricated /
    minimal) falls back to the event sources rather than failing.
    """
    if corr.correlation_type is CorrelationType.METRIC:
        metric_a = corr.details.get("metric_a")
        metric_b = corr.details.get("metric_b")
        if metric_a and metric_b:
            first, second = sorted((str(metric_a), str(metric_b)))
            return first, second
    first, second = sorted((corr.event_a.source.value, corr.event_b.source.value))
    return first, second


def pattern_key(corr: Correlation) -> PatternKey:
    """The full pattern identity: (correlation_type value, endpoint_a, endpoint_b)."""
    endpoint_a, endpoint_b = pattern_endpoints(corr)
    return (corr.correlation_type.value, endpoint_a, endpoint_b)


@dataclass
class PatternAssessment:
    """One correlation's verdict against its learned baseline (see assess())."""

    #: Prior observations of this pattern — BEFORE the assessed one.
    count: int
    #: Learned mean strength over those prior observations (0.0 when count == 0).
    avg_strength: float
    #: Confidence boost earned by recurrence, in [0, BOOST_CAP].
    boost: float
    #: True when an established pattern deviates > 2σ from its learned mean.
    is_anomalous: bool
    #: True on the very first sighting of an already-strong (>= 0.8) pattern.
    is_new: bool


class PatternLearner:
    """In-memory pattern baselines with lazy Redis hydration + mirroring."""

    def __init__(self, settings: Settings, store: RedisStore | None = None) -> None:
        self.settings = settings
        #: Optional Redis persistence; None (or a dead Redis) degrades to
        #: session-local learning — never an error, never a boost from thin air.
        self.store = store
        #: key -> {count, strength_sum, strength_sqsum, first_seen, last_seen}.
        #: None until the one-time lazy hydration on the first assess/record;
        #: THE source of truth for assess() (no Redis reads on the hot path).
        self._baselines: dict[PatternKey, dict[str, float]] | None = None

    # --- Assessment (hot path: dict lookups + arithmetic only) --------------------
    def assess(self, corr: Correlation, now: float) -> PatternAssessment:
        """Judge ``corr`` against the baseline BEFORE this observation.

        Call :meth:`record` afterwards to fold the observation in — that order
        means a pattern's Nth detection is always scored against the previous
        N-1, so the very first sighting earns zero boost. ``now`` is part of
        the stable learner signature (the engine passes its cycle clock);
        assessment itself is time-independent.
        """
        entry = self._ensure_loaded().get(pattern_key(corr))
        count = int(entry.get("count", 0)) if entry is not None else 0
        if entry is not None and count > 0:
            avg = float(entry.get("strength_sum", 0.0)) / count
            variance = max(0.0, float(entry.get("strength_sqsum", 0.0)) / count - avg * avg)
            sigma = math.sqrt(variance)
        else:
            count = 0
            avg = 0.0
            sigma = 0.0
        # log1p(0) == 0, so a first sighting gets exactly 0.0 boost.
        boost = min(BOOST_CAP, BOOST_SCALE * math.log1p(count))
        is_anomalous = count >= ANOMALY_MIN_COUNT and abs(
            corr.strength - avg
        ) > ANOMALY_SIGMA_MULTIPLIER * max(sigma, _SIGMA_FLOOR)
        is_new = count == 0 and corr.strength >= NEW_PATTERN_MIN_STRENGTH
        return PatternAssessment(
            count=count,
            avg_strength=avg,
            boost=boost,
            is_anomalous=is_anomalous,
            is_new=is_new,
        )

    # --- Learning -------------------------------------------------------------------
    def record(self, corrs: list[Correlation], now: float) -> None:
        """Fold a cycle's observations into the baselines and mirror them to Redis.

        The local dict is updated first (it is what assess() reads), then the
        whole batch is handed to :meth:`src.store.RedisStore.record_patterns`
        fire-and-forget — a failed mirror write costs persistence, never a cycle.
        """
        if not corrs:
            return
        baselines = self._ensure_loaded()
        updates: list[tuple[PatternKey, float, float]] = []
        for corr in corrs:
            key = pattern_key(corr)
            entry = baselines.get(key)
            if entry is None:
                entry = {
                    "count": 0,
                    "strength_sum": 0.0,
                    "strength_sqsum": 0.0,
                    "first_seen": now,
                    "last_seen": now,
                }
                baselines[key] = entry
            entry["count"] = int(entry.get("count", 0)) + 1
            entry["strength_sum"] = float(entry.get("strength_sum", 0.0)) + corr.strength
            entry["strength_sqsum"] = (
                float(entry.get("strength_sqsum", 0.0)) + corr.strength * corr.strength
            )
            entry["last_seen"] = now
            updates.append((key, float(corr.strength), now))
        if self.store is not None:
            # RedisStore degrades internally; the extra guard also survives
            # duck-typed stores in tests. Learning must never kill a cycle.
            try:
                self.store.record_patterns(updates)
            except Exception:  # noqa: BLE001 — degradation contract: never raise
                logger.warning("pattern mirror write failed; keeping local baselines only")

    # --- Lazy hydration ---------------------------------------------------------------
    def _ensure_loaded(self) -> dict[PatternKey, dict[str, float]]:
        """The baselines dict, hydrated from Redis exactly once (lazily).

        Any hydration failure degrades to an empty dict — zero boost until the
        patterns are re-learned live — and is attempted only this once, so a
        Redis outage cannot add a SCAN to every later cycle.
        """
        if self._baselines is None:
            loaded: dict[PatternKey, dict[str, float]] = {}
            if self.store is not None:
                # load_patterns() itself returns {} on failure; belt-and-braces
                # here so even a broken duck-typed store yields zero-boost, not
                # a crash.
                try:
                    loaded = self.store.load_patterns()
                except Exception:  # noqa: BLE001 — degradation contract: never raise
                    logger.warning("pattern baselines unavailable; starting empty")
            self._baselines = loaded
            if loaded:
                logger.info("hydrated %d pattern baselines from redis", len(loaded))
        return self._baselines
