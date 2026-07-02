"""Unit tests for the hybrid weighted-blend ranker (:mod:`src.ranker`).

Pure — no DB, no Redis, no embeddings, no HTTP. Every case that touches the
recency signal passes an **explicit** ``now`` and ``half_life_days`` (no ambient
clock, no ``freezegun``); every blend assertion passes an **explicit** ``weights``
mapping so the arithmetic is deterministic and independent of config drift. One
extra test exercises the config-default weights path (no ``weights=``) — it only
runs meaningfully in Docker where the settings deps are installed.

Synthetic candidates are built directly as :class:`src.retrieval.Candidate`
frozen dataclasses (all nine fields), so nothing here reaches the DB-backed
retrieval query path.

Covered (C8):

* :func:`blended_score` — the core ``w_sem*sem + w_ctx*ctx + w_fb*fb`` formula.
* :func:`rank_candidates` — semantic clamping into ``[0, 1]``, contextual blend,
  the ``feedback == 0.0`` stub, DESC ordering, the (score, semantic, incident_id)
  tie-break chain, ``top_k`` truncation, weight-sensitivity reordering, the
  ``breakdown`` contract, and the empty-input short-circuit.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.ranker import (
    QueryContext,
    RankedSuggestion,
    blended_score,
    rank_candidates,
)
from src.retrieval import Candidate

# Fixed reference "now" so every recency-dependent case is deterministic.
_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_HALF_LIFE = 30.0

# Explicit blend weights (mirror get_settings() defaults: 0.6 / 0.4 / 0.2).
_W = {"semantic": 0.6, "contextual": 0.4, "feedback": 0.2}


def _candidate(
    *,
    incident_id: int,
    semantic: float,
    service: str = "api",
    severity: str = "high",
    tags: list[str] | None = None,
    created_at: datetime | None = None,
) -> Candidate:
    """Build a synthetic :class:`Candidate` with sensible defaults.

    ``tags`` defaults to ``["db"]`` and ``created_at`` to ``_NOW`` so that, paired
    with a matching :class:`QueryContext`, every contextual sub-signal is 1.0 and
    the combined contextual score is exactly 1.0.
    """
    return Candidate(
        incident_id=incident_id,
        title=f"incident-{incident_id}",
        description=f"description for incident {incident_id}",
        service=service,
        severity=severity,
        tags=list(tags) if tags is not None else ["db"],
        resolution=f"resolution {incident_id}",
        created_at=created_at if created_at is not None else _NOW,
        semantic=semantic,
    )


def _matching_query() -> QueryContext:
    """A query whose facets match the ``_candidate`` defaults (contextual → 1.0)."""
    return QueryContext(service="api", severity="high", tags=["db"])


# --------------------------------------------------------------------------- #
# 1. Core blend math — blended_score in isolation
# --------------------------------------------------------------------------- #
def test_blended_score_semantic_only() -> None:
    # 0.6*1 + 0.4*0 + 0.2*0 = 0.6
    assert blended_score(1.0, 0.0, weights=_W) == pytest.approx(0.6)


def test_blended_score_contextual_only() -> None:
    # 0.6*0 + 0.4*1 + 0.2*0 = 0.4
    assert blended_score(0.0, 1.0, weights=_W) == pytest.approx(0.4)


def test_blended_score_half_half() -> None:
    # 0.6*0.5 + 0.4*0.5 + 0.2*0 = 0.3 + 0.2 = 0.5
    assert blended_score(0.5, 0.5, weights=_W) == pytest.approx(0.5)


def test_blended_score_feedback_only() -> None:
    # 0.6*0 + 0.4*0 + 0.2*1 = 0.2
    assert blended_score(0.0, 0.0, 1.0, weights=_W) == pytest.approx(0.2)


# --------------------------------------------------------------------------- #
# 2. Blend inside rank_candidates — known semantic, contextual forced to 1.0
# --------------------------------------------------------------------------- #
def test_rank_blend_with_full_contextual() -> None:
    """Matching query + candidate + ``created_at == now`` → contextual == 1.0.

    So score == 0.6*semantic + 0.4*1.0. With semantic 0.5 → 0.3 + 0.4 = 0.7.
    The feedback signal is the C8 stub, hence 0.0 on the result.
    """
    cand = _candidate(incident_id=1, semantic=0.5)
    (result,) = rank_candidates(
        [cand],
        _matching_query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )

    assert result.contextual == pytest.approx(1.0)
    assert result.semantic == pytest.approx(0.5)
    assert result.feedback == 0.0
    assert result.score == pytest.approx(0.6 * 0.5 + 0.4 * 1.0)  # 0.7


# --------------------------------------------------------------------------- #
# 3. Semantic clamping into [0, 1]
# --------------------------------------------------------------------------- #
def test_rank_clamps_negative_semantic() -> None:
    """A slightly-negative cosine similarity is clamped up to 0.0 before blending."""
    cand = _candidate(incident_id=1, semantic=-0.2)
    (result,) = rank_candidates(
        [cand],
        _matching_query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )
    assert result.semantic == 0.0
    # score uses the clamped 0.0: 0.6*0 + 0.4*1 = 0.4
    assert result.score == pytest.approx(0.4)


def test_rank_clamps_supraunit_semantic() -> None:
    """A > 1 semantic is clamped down to 1.0 before blending."""
    cand = _candidate(incident_id=1, semantic=1.5)
    (result,) = rank_candidates(
        [cand],
        _matching_query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )
    assert result.semantic == 1.0
    # score uses the clamped 1.0: 0.6*1 + 0.4*1 = 1.0
    assert result.score == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# 4. Descending order by blended score
# --------------------------------------------------------------------------- #
def test_rank_orders_by_score_descending() -> None:
    """Three candidates with distinct semantics (contextual equal) → score DESC.

    All share full contextual (1.0), so the score ordering follows semantic:
    id 2 (0.9) > id 3 (0.5) > id 1 (0.1). Input order is deliberately shuffled.
    """
    cands = [
        _candidate(incident_id=1, semantic=0.1),
        _candidate(incident_id=2, semantic=0.9),
        _candidate(incident_id=3, semantic=0.5),
    ]
    results = rank_candidates(
        cands, _matching_query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )

    assert [r.incident_id for r in results] == [2, 3, 1]
    # Scores are non-increasing.
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)


# --------------------------------------------------------------------------- #
# 5. Tie-breaking: semantic DESC, then incident_id ASC
# --------------------------------------------------------------------------- #
def test_rank_tiebreak_by_semantic_when_scores_equal() -> None:
    """Equal blended score but differing semantic → higher semantic ranks first.

    Both candidates match contextual (1.0). To make the *scores* equal while the
    semantics differ, weight semantic at 0.0 so score == 0.4*1.0 for both; the
    tie-break then falls to semantic DESC.
    """
    weights = {"semantic": 0.0, "contextual": 0.4, "feedback": 0.0}
    cands = [
        _candidate(incident_id=1, semantic=0.2),
        _candidate(incident_id=2, semantic=0.8),
    ]
    results = rank_candidates(
        cands, _matching_query(), weights=weights, half_life_days=_HALF_LIFE, now=_NOW
    )

    # Scores tie exactly; higher semantic (id 2) wins the first tie-break.
    assert results[0].score == pytest.approx(results[1].score)
    assert [r.incident_id for r in results] == [2, 1]


def test_rank_tiebreak_by_incident_id_when_score_and_semantic_equal() -> None:
    """Equal score AND equal semantic → lower incident_id ranks first.

    Identical semantics and full contextual make every field of the sort key tie
    except incident_id; the ascending id tie-break makes the order deterministic
    regardless of input order (here 3, 1, 2 in → 1, 2, 3 out).
    """
    cands = [
        _candidate(incident_id=3, semantic=0.5),
        _candidate(incident_id=1, semantic=0.5),
        _candidate(incident_id=2, semantic=0.5),
    ]
    results = rank_candidates(
        cands, _matching_query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )

    assert [r.incident_id for r in results] == [1, 2, 3]
    # Every score is identical (same semantic, same contextual).
    assert len({round(r.score, 12) for r in results}) == 1


# --------------------------------------------------------------------------- #
# 6. top_k truncation
# --------------------------------------------------------------------------- #
def test_rank_top_k_truncates_to_top_two() -> None:
    """``top_k=2`` returns exactly the two highest-scoring candidates."""
    cands = [
        _candidate(incident_id=i, semantic=s)
        for i, s in [(1, 0.1), (2, 0.9), (3, 0.5), (4, 0.7), (5, 0.3)]
    ]
    results = rank_candidates(
        cands,
        _matching_query(),
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )

    assert len(results) == 2
    # Highest two semantics are 0.9 (id 2) then 0.7 (id 4).
    assert [r.incident_id for r in results] == [2, 4]


def test_rank_default_top_k_caps_at_five() -> None:
    """With no ``top_k`` the default (settings.top_k == 5) caps a 7-candidate pool."""
    cands = [
        _candidate(incident_id=i, semantic=i / 10.0) for i in range(1, 8)  # 7 candidates
    ]
    results = rank_candidates(
        cands, _matching_query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )
    assert len(results) <= 5
    assert len(results) == 5  # default top_k is 5


# --------------------------------------------------------------------------- #
# 7. Weight sensitivity — reordering when weights change
# --------------------------------------------------------------------------- #
def test_rank_weight_sensitivity_reorders() -> None:
    """The winner flips when the blend weights shift semantic <-> contextual.

    A: high semantic (0.9), low contextual (service-only match → not full).
    B: low semantic (0.1), full contextual (all facets match).

    With weights favouring semantic (0.9/0.1) A wins; favouring contextual
    (0.1/0.9) B wins. Feedback weight 0 keeps it a clean two-signal contrast.
    """
    query = _matching_query()  # service=api, severity=high, tags=["db"]

    # A matches only service → contextual < 1.0; high semantic.
    cand_a = _candidate(
        incident_id=1,
        semantic=0.9,
        service="api",
        severity="low",  # far from "high"
        tags=["unrelated"],  # no tag overlap
        created_at=_NOW - timedelta(days=365),  # stale → low recency
    )
    # B matches every facet + fresh → contextual == 1.0; low semantic.
    cand_b = _candidate(incident_id=2, semantic=0.1)

    sem_heavy = {"semantic": 0.9, "contextual": 0.1, "feedback": 0.0}
    ctx_heavy = {"semantic": 0.1, "contextual": 0.9, "feedback": 0.0}

    sem_first = rank_candidates(
        [cand_a, cand_b], query, weights=sem_heavy, half_life_days=_HALF_LIFE, now=_NOW
    )
    ctx_first = rank_candidates(
        [cand_a, cand_b], query, weights=ctx_heavy, half_life_days=_HALF_LIFE, now=_NOW
    )

    assert sem_first[0].incident_id == 1  # A wins when semantic dominates
    assert ctx_first[0].incident_id == 2  # B wins when contextual dominates


# --------------------------------------------------------------------------- #
# 8. breakdown contract
# --------------------------------------------------------------------------- #
def test_rank_breakdown_contract() -> None:
    """The per-suggestion breakdown carries all signals, the weights, and detail."""
    cand = _candidate(incident_id=1, semantic=0.5)
    (result,) = rank_candidates(
        [cand],
        _matching_query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )
    bd = result.breakdown

    # Exact top-level key set. ``base`` (the feedback-free semantic+contextual signal)
    # was added to the breakdown in C12 alongside the exploration/diversity guards.
    assert set(bd) == {
        "semantic",
        "contextual",
        "feedback",
        "base",
        "contextual_detail",
        "weights",
    }

    # feedback stub is 0.0; weights echo what was passed in.
    assert bd["feedback"] == 0.0
    assert bd["weights"] == _W
    # ``base`` == w_sem*semantic + w_ctx*contextual (no feedback term).
    assert bd["base"] == pytest.approx(
        _W["semantic"] * result.semantic + _W["contextual"] * result.contextual
    )

    # breakdown["semantic"] mirrors the (clamped) semantic on the suggestion.
    assert bd["semantic"] == result.semantic
    assert bd["contextual"] == result.contextual

    # contextual_detail exposes the four sub-signals + their weights.
    detail = bd["contextual_detail"]
    assert {"service", "severity", "tags", "recency", "weights"} <= set(detail)


def test_rank_default_weights_breakdown_matches_config() -> None:
    """Without ``weights=`` the blend uses config defaults (0.6/0.4/0.2).

    Exercises the config-default path (settings loaded in Docker). The breakdown's
    recorded weights must equal the documented defaults, and feedback stays 0.0.
    """
    cand = _candidate(incident_id=1, semantic=0.5)
    (result,) = rank_candidates(
        [cand], _matching_query(), half_life_days=_HALF_LIFE, now=_NOW
    )

    assert result.breakdown["weights"] == {
        "semantic": 0.6,
        "contextual": 0.4,
        "feedback": 0.2,
    }
    assert result.breakdown["feedback"] == 0.0
    # Sanity: default-weighted score with full contextual == 0.6*0.5 + 0.4*1.0.
    assert result.score == pytest.approx(0.6 * 0.5 + 0.4 * 1.0)


# --------------------------------------------------------------------------- #
# 9. Empty input short-circuit
# --------------------------------------------------------------------------- #
def test_rank_empty_candidates_returns_empty() -> None:
    assert rank_candidates([], QueryContext(service="api")) == []


# --------------------------------------------------------------------------- #
# Result-type sanity — rank_candidates yields RankedSuggestion instances
# --------------------------------------------------------------------------- #
def test_rank_returns_ranked_suggestions() -> None:
    cand = _candidate(incident_id=7, semantic=0.5)
    (result,) = rank_candidates(
        [cand], _matching_query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )
    assert isinstance(result, RankedSuggestion)
    # Candidate fields propagate onto the suggestion unchanged.
    assert result.incident_id == 7
    assert result.title == "incident-7"
    assert result.created_at == _NOW
