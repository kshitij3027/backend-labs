"""Contextual (non-semantic) similarity scoring — pure functions.

This is the *structured-signal* stage of the recommendation pipeline. Given the
plain incident fields of a query and a candidate it derives four independent
signals — service match, severity proximity, tag overlap, and recency — each a
float in ``[0, 1]``, and combines them into a single ``contextual`` score via a
weighted average. These signals capture agreement the embedding vector cannot
express directly (same service, close severity, shared tags, freshness).

Scope (C7)
----------
Pure functions only: **no** database, **no** embeddings, **no** HTTP endpoint,
and — importantly — **no blending with the semantic score** (that is C8). Every
function here is side-effect free and deterministic given its inputs, which is
exactly what makes them cheap to unit-test. Time is injectable: the recency
signal takes an explicit ``now`` so tests never need to freeze the clock (no
``freezegun`` dependency).

Vocabulary
----------
Severity ordering is **not** redefined here: it is imported from
:data:`src.schemas.SEVERITIES` (``["critical", "high", "medium", "low"]``,
most → least severe), the single source of truth shared with the generator and
the API contract. The ordinal position is what the severity-proximity signal
measures distance over.

Score convention
----------------
Every individual signal and the combined score lie in ``[0, 1]``; higher = more
contextually similar. The combined score is a weighted **average** (weights are
normalised to sum to 1), so it stays in ``[0, 1]`` regardless of how the raw
``ctx_weight_*`` config values are scaled.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.schemas import SEVERITIES

if TYPE_CHECKING:  # typing-only imports; avoid an import cycle with retrieval at runtime
    from src.retrieval import Candidate

# Precomputed severity -> ordinal-rank map (index in SEVERITIES; 0 = most severe).
# Built once at import from the canonical ordering so lookups are O(1) and we never
# duplicate the severity vocabulary. Normalised to lower-case keys for robustness.
_SEVERITY_RANK: dict[str, int] = {name.lower(): i for i, name in enumerate(SEVERITIES)}

# Denominator for severity-distance normalisation: the largest possible rank gap.
# Guarded against a degenerate one-element severity list (never divide by zero).
_SEVERITY_SPAN: int = max(len(SEVERITIES) - 1, 1)


# --------------------------------------------------------------------------- #
# Individual signals — each pure, each returns a float in [0, 1]
# --------------------------------------------------------------------------- #
def service_match(query_service: str | None, cand_service: str | None) -> float:
    """1.0 when the two services match case-insensitively, else 0.0.

    Deliberately a flat exact match — no service hierarchy or fuzzy matching.
    A missing/blank query or candidate service can never match, so it scores 0.0.
    """
    if not query_service or not cand_service:
        return 0.0
    return 1.0 if query_service.strip().lower() == cand_service.strip().lower() else 0.0


def severity_proximity(query_severity: str | None, cand_severity: str | None) -> float:
    """Closeness of two severities on the ordinal :data:`SEVERITIES` scale.

    Uses the ordinal rank (index in ``SEVERITIES``)::

        1 - abs(rank_q - rank_c) / (len(SEVERITIES) - 1)

    So identical severities → ``1.0``, adjacent → ``~0.667`` (for the 4-level
    scale), and the opposite ends (``critical`` vs ``low``) → ``0.0``.

    If either severity is missing or not in the known vocabulary the signal is
    ``0.0`` (treated as "no evidence of proximity" rather than a neutral 0.5 —
    an unknown severity contributes nothing to the contextual match).
    """
    if not query_severity or not cand_severity:
        return 0.0
    rank_q = _SEVERITY_RANK.get(query_severity.strip().lower())
    rank_c = _SEVERITY_RANK.get(cand_severity.strip().lower())
    if rank_q is None or rank_c is None:
        return 0.0
    return 1.0 - abs(rank_q - rank_c) / _SEVERITY_SPAN


def tag_jaccard(
    query_tags: list[str] | None, cand_tags: list[str] | None
) -> float:
    """Jaccard overlap ``|∩| / |∪|`` of two tag sets.

    Tags are case-normalised (lower-cased) and stripped before comparison, and
    blanks are dropped. An empty union (either side empty, or all-blank tags on
    both) yields ``0.0`` — with no shared vocabulary there is no measurable
    overlap.
    """
    q = {t.strip().lower() for t in (query_tags or []) if t and t.strip()}
    c = {t.strip().lower() for t in (cand_tags or []) if t and t.strip()}
    union = q | c
    if not union:
        return 0.0
    return len(q & c) / len(union)


def recency_decay(
    created_at: datetime,
    *,
    now: datetime | None = None,
    half_life_days: float | None = None,
) -> float:
    """Exponential half-life decay of a candidate's age, in ``[0, 1]``.

    Computed as ``0.5 ** (age_days / half_life_days)`` where
    ``age_days = max(0, (now - created_at) / 1 day)``. So age ``0`` → ``1.0``,
    age == one half-life → ``0.5``, age == two half-lives → ``0.25``. Future
    timestamps (negative age) are clamped to age ``0`` → ``1.0``.

    Parameters
    ----------
    created_at:
        When the candidate incident was created.
    now:
        Reference "current" time. Defaults to ``datetime.now(timezone.utc)``.
        Injectable so tests can pass a fixed reference without freezing the clock.
    half_life_days:
        Age in days at which the signal halves. Defaults to
        ``get_settings().recency_half_life_days``. A non-positive half-life is
        treated as "no decay" and returns ``1.0`` for any non-future age (avoids
        division/overflow on a misconfigured value).

    Notes
    -----
    Timezone handling is defensive: if exactly one of ``created_at`` / ``now`` is
    tz-aware and the other naive, the naive one is assumed to be UTC so the
    subtraction never raises. This keeps the pure function robust to callers that
    pass naive datetimes in tests.
    """
    reference = now if now is not None else datetime.now(timezone.utc)

    if half_life_days is None:
        # Read config lazily (inside the function) so importing this module never
        # forces settings parsing — keeps the module importable without deps.
        from src.config import get_settings

        half_life_days = get_settings().recency_half_life_days

    created_at, reference = _align_timezones(created_at, reference)

    age_seconds = (reference - created_at).total_seconds()
    age_days = max(0.0, age_seconds / 86400.0)

    if half_life_days <= 0:
        # Degenerate config: no meaningful decay. Fresh/any past incident → 1.0.
        return 1.0

    return 0.5 ** (age_days / half_life_days)


def _align_timezones(a: datetime, b: datetime) -> tuple[datetime, datetime]:
    """Return ``(a, b)`` made mutually subtractable.

    If one is tz-aware and the other naive, the naive one is assumed UTC. If both
    are naive or both aware, they are returned unchanged (their difference is
    already well-defined).
    """
    a_aware = a.tzinfo is not None
    b_aware = b.tzinfo is not None
    if a_aware == b_aware:
        return a, b
    if not a_aware:
        a = a.replace(tzinfo=timezone.utc)
    if not b_aware:
        b = b.replace(tzinfo=timezone.utc)
    return a, b


# --------------------------------------------------------------------------- #
# Combined contextual score
# --------------------------------------------------------------------------- #
def _resolve_weights(weights: dict[str, float] | None) -> dict[str, float]:
    """Return the four contextual sub-signal weights, defaulting from config.

    When ``weights`` is ``None`` the ``ctx_weight_*`` fields on
    :func:`src.config.get_settings` are used (read lazily). A provided mapping may
    be partial; any missing signal defaults to weight ``0.0``.
    """
    if weights is None:
        from src.config import get_settings

        return dict(get_settings().contextual_weights)
    return {
        "service": float(weights.get("service", 0.0)),
        "severity": float(weights.get("severity", 0.0)),
        "tags": float(weights.get("tags", 0.0)),
        "recency": float(weights.get("recency", 0.0)),
    }


def contextual_score(
    *,
    query_service: str | None,
    query_severity: str | None,
    query_tags: list[str] | None,
    cand_service: str | None,
    cand_severity: str | None,
    cand_tags: list[str] | None,
    cand_created_at: datetime,
    weights: dict[str, float] | None = None,
    half_life_days: float | None = None,
    now: datetime | None = None,
) -> tuple[float, dict]:
    """Combine the four contextual signals into one score in ``[0, 1]``.

    Computes :func:`service_match`, :func:`severity_proximity`,
    :func:`tag_jaccard` and :func:`recency_decay`, then returns their
    **weight-normalised weighted average** using the ``ctx_weight_*`` config
    weights (or an explicit ``weights`` mapping). Normalising by the weight sum
    guarantees the result stays in ``[0, 1]`` even when the raw weights do not sum
    to 1. If every weight is ``0`` (or negative-summing), the score falls back to
    the unweighted mean of the four signals.

    Returns
    -------
    tuple[float, dict]
        ``(score, breakdown)`` where ``breakdown`` is::

            {
                "service":  <float>,   # raw signal values
                "severity": <float>,
                "tags":     <float>,
                "recency":  <float>,
                "weights":  {"service": ..., "severity": ...,
                             "tags": ..., "recency": ...},  # weights actually used
            }
    """
    signals = {
        "service": service_match(query_service, cand_service),
        "severity": severity_proximity(query_severity, cand_severity),
        "tags": tag_jaccard(query_tags, cand_tags),
        "recency": recency_decay(
            cand_created_at, now=now, half_life_days=half_life_days
        ),
    }

    resolved = _resolve_weights(weights)
    total = sum(resolved.values())

    if total > 0:
        score = sum(signals[k] * resolved[k] for k in signals) / total
    else:
        # No usable weights — degrade to a plain average so the score is still a
        # sensible [0, 1] summary rather than 0 or a divide-by-zero.
        score = sum(signals.values()) / len(signals)

    # Clamp defensively against floating-point drift so the contract [0, 1] holds.
    score = min(1.0, max(0.0, score))

    breakdown = {**signals, "weights": resolved}
    return score, breakdown


def score_candidate(
    query_ctx: object,
    candidate: "Candidate",
    *,
    weights: dict[str, float] | None = None,
    half_life_days: float | None = None,
    now: datetime | None = None,
) -> tuple[float, dict]:
    """Convenience wrapper: score a :class:`~src.retrieval.Candidate` against a query.

    ``query_ctx`` is any object exposing ``service`` / ``severity`` / ``tags``
    attributes (e.g. an incident-like query model); ``candidate`` is a retrieval
    :class:`~src.retrieval.Candidate`. This simply unpacks both into the primitive
    :func:`contextual_score` — which remains the tested core — so callers in later
    commits don't have to spell out every field.

    Returns the same ``(score, breakdown)`` pair as :func:`contextual_score`.
    """
    return contextual_score(
        query_service=getattr(query_ctx, "service", None),
        query_severity=getattr(query_ctx, "severity", None),
        query_tags=getattr(query_ctx, "tags", None),
        cand_service=candidate.service,
        cand_severity=candidate.severity,
        cand_tags=candidate.tags,
        cand_created_at=candidate.created_at,
        weights=weights,
        half_life_days=half_life_days,
        now=now,
    )


__all__ = [
    "service_match",
    "severity_proximity",
    "tag_jaccard",
    "recency_decay",
    "contextual_score",
    "score_candidate",
]
