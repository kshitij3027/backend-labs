"""Context-mode ranking helpers.

Commit 08's reranker composes the other primitives using a weight
vector resolved from the current ``context.mode``. The default mode
uses ``settings.ranking_weights`` plus the long
``temporal_half_life_normal_s``; ``incident`` mode overrides both of
those with the shorter-half-life / higher-severity-weight preset
from ``settings.incident_ranking_weights``. Unknown modes fall
through to the defaults so an exotic client-supplied value simply
yields the baseline ranking instead of crashing.
"""

from __future__ import annotations

from typing import TypedDict

from src.config import Settings


class EffectiveWeights(TypedDict):
    """Resolved weight vector + half-life for a given mode.

    Declared as a :class:`TypedDict` rather than a dataclass so the
    reranker can treat it as a plain dict for weighted-sum arithmetic
    while still getting static-type support for the known keys.
    """

    tfidf: float
    temporal: float
    severity: float
    service: float
    context: float
    half_life_s: int
    mode: str | None


def effective_weights(mode: str | None, settings: Settings) -> EffectiveWeights:
    """Resolve ``mode`` to a concrete weight vector + temporal half-life.

    ``mode == "incident"`` pulls ``incident_ranking_weights`` and the
    shorter ``temporal_half_life_incident_s``. Every other mode (or
    ``None``) uses the defaults. Extra modes are easy to add here
    without touching the reranker.
    """
    if mode == "incident":
        weights = settings.incident_ranking_weights
        half_life = settings.temporal_half_life_incident_s
    else:
        weights = settings.ranking_weights
        half_life = settings.temporal_half_life_normal_s
    return EffectiveWeights(
        tfidf=float(weights.get("tfidf", 0.0)),
        temporal=float(weights.get("temporal", 0.0)),
        severity=float(weights.get("severity", 0.0)),
        service=float(weights.get("service", 0.0)),
        context=float(weights.get("context", 0.0)),
        half_life_s=int(half_life),
        mode=mode,
    )


def context_bonus(mode: str | None, level: str) -> float:
    """Flat additive boost applied when ``mode`` matches a hot signal.

    Currently: ``incident + ERROR/FATAL`` -> ``1.0``; anything else
    -> ``0.0``. Expanding the rules is intentionally cheap — add
    another branch rather than plumbing a table through settings.
    """
    if not mode:
        return 0.0
    if mode == "incident" and level.upper() in ("ERROR", "FATAL"):
        return 1.0
    return 0.0
