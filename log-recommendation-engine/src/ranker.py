"""Hybrid ranker — weighted blend of the semantic and contextual signals.

This is the *re-ranking* stage of the recommendation pipeline. It takes the wide
pool of already-retrieved :class:`~src.retrieval.Candidate` objects (the semantic
K-NN output of :mod:`src.retrieval`), scores each one's structured
:func:`~src.contextual.contextual_score` against the query, and blends the two
into a single relevance ``score`` via a fixed weighted sum. Candidates are then
sorted best-first and truncated to ``top_k``, each carrying a per-signal
``breakdown`` so the endpoint (C9) and the dashboard can explain *why* a
suggestion ranked where it did.

Scope (C8 + C11)
----------------
Pure re-ranking only: **no** database, **no** Redis, **no** embeddings and **no**
HTTP endpoint. :func:`rank_candidates` receives candidates a caller already
retrieved and never performs I/O, which keeps it deterministic and cheap to
unit-test. The **feedback** signal is supplied *by the caller* as a
``feedback_scores`` mapping (``incident_id -> net_help``, C11): the composition root
in :mod:`src.recommendation_service` reads the per-pattern ``suggestion_scores`` and
passes the smoothed net-helpfulness in. When that mapping is omitted the feedback
term is ``0.0`` for every candidate, so this module still performs no I/O and behaves
exactly as it did in C8.

Blend convention
----------------
Each input signal is a similarity in ``[0, 1]`` (semantic is *clamped* into that
range — a cosine similarity can dip slightly negative for unrelated text). The
blended relevance is::

    base  = w_semantic * semantic + w_contextual * contextual
    score = base + w_feedback * feedback          # feedback == 0.0 in C8

so while ``feedback`` is 0 the score equals ``base`` and stays in ``[0, 1]``
whenever the semantic/contextual weights themselves sum to ≤ 1 (they default to
0.6 + 0.4 = 1.0). Weights come from an explicit ``weights`` mapping or, by
default, the ``weight_*`` fields on :func:`src.config.get_settings`.

Ordering convention
-------------------
Results are sorted by blended ``score`` **descending**. Ties are broken
deterministically by ``semantic`` descending, then ``incident_id`` ascending, so
the ranking is stable and reproducible across runs regardless of input order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from src import contextual

if TYPE_CHECKING:  # typing-only; avoids importing the DB-backed retrieval module at runtime
    from src.retrieval import Candidate


# --------------------------------------------------------------------------- #
# Small value objects
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class QueryContext:
    """The structured (non-semantic) query facets the contextual signals score against.

    Mirrors the three fields :func:`src.contextual.contextual_score` consumes on
    the query side. Kept intentionally tiny and dependency-free so it is trivial to
    construct in tests and from the API request model in later commits. Any of the
    fields may be ``None`` / empty — the underlying signals treat missing facets as
    "no evidence" (score 0), never as an error.
    """

    service: str | None = None
    severity: str | None = None
    tags: list[str] | None = None


@dataclass(frozen=True)
class RankedSuggestion:
    """One re-ranked incident: the candidate's fields plus its blended relevance.

    ``score`` is the final blended relevance (``w_sem*semantic + w_ctx*contextual
    + w_fb*feedback``); ``semantic`` / ``contextual`` / ``feedback`` are the three
    individual signals actually fed into the blend (``semantic`` post-clamp,
    ``feedback`` a 0.0 stub in C8). ``breakdown`` carries the same values plus the
    contextual sub-signal detail and the blend weights used, so the suggestion is
    fully self-explaining downstream.
    """

    incident_id: int
    title: str
    description: str
    service: str
    severity: str
    tags: list[str]
    resolution: str
    created_at: datetime
    score: float
    semantic: float
    contextual: float
    feedback: float
    breakdown: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _clamp01(value: float) -> float:
    """Clamp ``value`` into the closed unit interval ``[0.0, 1.0]``."""
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _resolve_blend_weights(weights: dict[str, float] | None) -> dict[str, float]:
    """Return the three blend weights (``semantic``/``contextual``/``feedback``).

    When ``weights`` is ``None`` the ``weight_*`` fields on
    :func:`src.config.get_settings` are used (read lazily so importing this module
    never forces settings parsing — the host may lack the settings deps). A provided
    mapping may be partial; any missing signal defaults to weight ``0.0``.
    """
    if weights is None:
        # Lazy import + call so ``import src.ranker`` stays dependency-free.
        from src.config import get_settings

        settings = get_settings()
        return {
            "semantic": float(settings.weight_semantic),
            "contextual": float(settings.weight_contextual),
            "feedback": float(settings.weight_feedback),
        }
    return {
        "semantic": float(weights.get("semantic", 0.0)),
        "contextual": float(weights.get("contextual", 0.0)),
        "feedback": float(weights.get("feedback", 0.0)),
    }


def blended_score(
    semantic: float,
    contextual: float,
    feedback: float = 0.0,
    *,
    weights: dict[str, float] | None = None,
) -> float:
    """Blend the three signals into one relevance score — the core formula.

    Computes ``w_semantic*semantic + w_contextual*contextual + w_feedback*feedback``.
    Exposed on its own (separate from :func:`rank_candidates`) so the math is
    unit-testable directly without constructing candidates. Weights default to the
    ``weight_*`` config fields; pass an explicit ``weights`` mapping to override.

    The three signal inputs are expected to already lie in ``[0, 1]``
    (:func:`rank_candidates` clamps ``semantic`` before calling this). The result
    is **not** re-clamped here — with the default weights (0.6/0.4/0.2) and
    in-range signals it stays in ``[0, 1]`` while ``feedback == 0``; leaving it
    unclamped keeps the helper a faithful mirror of the raw weighted sum for tests.
    """
    w = _resolve_blend_weights(weights)
    return (
        w["semantic"] * semantic
        + w["contextual"] * contextual
        + w["feedback"] * feedback
    )


# --------------------------------------------------------------------------- #
# Ranking
# --------------------------------------------------------------------------- #
def rank_candidates(
    candidates: list["Candidate"],
    query: QueryContext,
    *,
    weights: dict[str, float] | None = None,
    top_k: int | None = None,
    half_life_days: float | None = None,
    now: datetime | None = None,
    feedback_scores: dict[int, float] | None = None,
) -> list[RankedSuggestion]:
    """Re-rank retrieved ``candidates`` by a blended relevance score, best-first.

    For each candidate this:

    1. clamps its ``semantic`` similarity into ``[0, 1]`` (cosine can be slightly
       negative for unrelated text, which would otherwise drag the blend below 0);
    2. computes the structured ``contextual`` score of ``query`` vs the candidate
       via :func:`src.contextual.contextual_score` (threading ``half_life_days`` /
       ``now`` through to the recency signal);
    3. looks up the candidate's **net-helpfulness feedback** term from
       ``feedback_scores`` by ``incident_id`` (``0.0`` when absent — no votes for
       this ``(pattern, incident)``);
    4. blends them with :func:`blended_score`.

    The list is then sorted by blended ``score`` **descending**, tie-broken by
    ``semantic`` descending then ``incident_id`` ascending for a stable,
    reproducible order, and truncated to ``top_k`` (default ``settings.top_k``).

    ``feedback_scores`` maps ``incident_id -> net_help`` (each already smoothed and
    bounded in ``[-1, 1]`` by :func:`src.feedback.net_help`), so the caller
    (:mod:`src.recommendation_service`) folds the learned per-pattern signal into the
    blend. It is **not** re-clamped here: a strong ±feedback term can nudge the final
    ``score`` slightly outside ``[0, 1]``, which is fine — only the *ordering*
    matters, and the deterministic tie-break keeps it stable. When ``None`` / empty,
    every feedback term is ``0.0`` and this behaves exactly as the pre-C11 ranker.

    Pure: no DB / Redis / embedding access. ``weights`` overrides the config blend
    weights (see :func:`blended_score`); an empty ``candidates`` list yields ``[]``.
    """
    feedback_by_incident = feedback_scores or {}
    # Resolve blend weights once (not per candidate) — same weights label the whole
    # ranking and ride along in every breakdown.
    blend_weights = _resolve_blend_weights(weights)

    scored: list[RankedSuggestion] = []
    for cand in candidates:
        semantic = _clamp01(cand.semantic)

        contextual_val, ctx_breakdown = contextual.contextual_score(
            query_service=query.service,
            query_severity=query.severity,
            query_tags=query.tags,
            cand_service=cand.service,
            cand_severity=cand.severity,
            cand_tags=cand.tags,
            cand_created_at=cand.created_at,
            half_life_days=half_life_days,
            now=now,
        )

        # Per-candidate learned feedback (C11): smoothed net-helpfulness for this
        # (query pattern, incident), already bounded [-1, 1]; 0.0 when no votes exist.
        feedback = feedback_by_incident.get(cand.incident_id, 0.0)

        score = blended_score(
            semantic, contextual_val, feedback, weights=blend_weights
        )

        breakdown = {
            "semantic": semantic,
            "contextual": contextual_val,
            "feedback": feedback,
            "contextual_detail": ctx_breakdown,
            "weights": dict(blend_weights),
        }

        scored.append(
            RankedSuggestion(
                incident_id=cand.incident_id,
                title=cand.title,
                description=cand.description,
                service=cand.service,
                severity=cand.severity,
                tags=list(cand.tags or []),
                resolution=cand.resolution,
                created_at=cand.created_at,
                score=score,
                semantic=semantic,
                contextual=contextual_val,
                feedback=feedback,
                breakdown=breakdown,
            )
        )

    # Deterministic order: score DESC, then semantic DESC, then incident_id ASC.
    # Python's sort is stable, so a single key that negates the "descending" fields
    # yields the full tie-break chain in one pass.
    scored.sort(key=lambda s: (-s.score, -s.semantic, s.incident_id))

    limit = top_k if top_k is not None else _default_top_k()
    return scored[:limit]


def _default_top_k() -> int:
    """Return the configured default ``top_k`` (read lazily to stay import-light)."""
    from src.config import get_settings

    return get_settings().top_k


__all__ = [
    "QueryContext",
    "RankedSuggestion",
    "blended_score",
    "rank_candidates",
]
