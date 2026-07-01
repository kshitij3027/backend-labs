"""Unit tests for the contextual (non-semantic) similarity scorer
(:mod:`src.contextual`).

Pure — no DB, no embeddings, no HTTP, no ambient clock. Every recency-dependent
case passes an **explicit** ``now`` and ``half_life_days`` so the tests never rely
on the wall clock (no ``freezegun``). Repeating decimals (e.g. severity ``2/3``,
the combined ``0.73333``) are asserted with :func:`pytest.approx`.

Covered (C7):

* :func:`service_match` — case-insensitive exact match; missing side → 0.0.
* :func:`severity_proximity` — ordinal distance over
  ``SEVERITIES=["critical","high","medium","low"]``; unknown/None → 0.0.
* :func:`tag_jaccard` — normalised-set Jaccard; empty union → 0.0.
* :func:`recency_decay` — ``0.5 ** (age / half_life)`` with age clamped ≥ 0;
  naive ``created_at`` assumed UTC against an aware ``now``.
* :func:`contextual_score` — weight-normalised weighted average, its
  ``breakdown`` contract, weight-scale invariance, and the all-zero-weights mean
  fallback.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.contextual import (
    contextual_score,
    recency_decay,
    service_match,
    severity_proximity,
    tag_jaccard,
)

# A fixed reference "now" so every recency assertion is deterministic.
_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_HALF_LIFE = 30.0

# Default contextual weights (mirror get_settings().contextual_weights).
_DEFAULT_WEIGHTS = {"service": 0.4, "severity": 0.2, "tags": 0.25, "recency": 0.15}


# --------------------------------------------------------------------------- #
# service_match
# --------------------------------------------------------------------------- #
def test_service_match_exact() -> None:
    assert service_match("api", "api") == 1.0


def test_service_match_case_insensitive() -> None:
    assert service_match("API", "api") == 1.0


def test_service_match_different() -> None:
    assert service_match("api", "web") == 0.0


def test_service_match_none_query() -> None:
    assert service_match(None, "api") == 0.0


# --------------------------------------------------------------------------- #
# severity_proximity
# --------------------------------------------------------------------------- #
def test_severity_proximity_identical() -> None:
    assert severity_proximity("high", "high") == 1.0


def test_severity_proximity_one_step() -> None:
    # critical(0) vs high(1): 1 - 1/3 = 2/3
    assert severity_proximity("critical", "high") == pytest.approx(2 / 3)


def test_severity_proximity_two_steps() -> None:
    # critical(0) vs medium(2): 1 - 2/3 = 1/3
    assert severity_proximity("critical", "medium") == pytest.approx(1 / 3)


def test_severity_proximity_opposite_ends() -> None:
    # critical(0) vs low(3): 1 - 3/3 = 0.0
    assert severity_proximity("critical", "low") == 0.0


def test_severity_proximity_unknown() -> None:
    assert severity_proximity("bogus", "low") == 0.0


def test_severity_proximity_none() -> None:
    assert severity_proximity(None, "low") == 0.0


# --------------------------------------------------------------------------- #
# tag_jaccard
# --------------------------------------------------------------------------- #
def test_tag_jaccard_partial_overlap() -> None:
    # {a,b,c} ∩ {b,c,d} = {b,c} (2); ∪ = {a,b,c,d} (4) → 0.5
    assert tag_jaccard(["a", "b", "c"], ["b", "c", "d"]) == 0.5


def test_tag_jaccard_normalised_full_overlap() -> None:
    # case/whitespace-normalised: {a,b} == {a,b} → 1.0
    assert tag_jaccard(["A", " b "], ["a", "B"]) == 1.0


def test_tag_jaccard_disjoint() -> None:
    assert tag_jaccard(["a"], ["x"]) == 0.0


def test_tag_jaccard_one_empty() -> None:
    assert tag_jaccard([], ["a"]) == 0.0


def test_tag_jaccard_both_none() -> None:
    assert tag_jaccard(None, None) == 0.0


# --------------------------------------------------------------------------- #
# recency_decay (explicit now + half_life_days — no ambient clock)
# --------------------------------------------------------------------------- #
def test_recency_decay_age_zero() -> None:
    assert recency_decay(_NOW, now=_NOW, half_life_days=_HALF_LIFE) == 1.0


def test_recency_decay_one_half_life() -> None:
    created = _NOW - timedelta(days=30)
    assert recency_decay(created, now=_NOW, half_life_days=_HALF_LIFE) == pytest.approx(0.5)


def test_recency_decay_two_half_lives() -> None:
    created = _NOW - timedelta(days=60)
    assert recency_decay(created, now=_NOW, half_life_days=_HALF_LIFE) == pytest.approx(0.25)


def test_recency_decay_future_clamped() -> None:
    # created_at in the future → negative age clamped to 0 → 1.0
    created = _NOW + timedelta(days=10)
    assert recency_decay(created, now=_NOW, half_life_days=_HALF_LIFE) == 1.0


def test_recency_decay_naive_created_at_assumed_utc() -> None:
    # naive created_at 30 days before an aware now → treated as UTC → 0.5
    naive_created = (_NOW - timedelta(days=30)).replace(tzinfo=None)
    assert recency_decay(
        naive_created, now=_NOW, half_life_days=_HALF_LIFE
    ) == pytest.approx(0.5)


# --------------------------------------------------------------------------- #
# contextual_score — combined weighted average + contract
# --------------------------------------------------------------------------- #
def test_contextual_score_full_example() -> None:
    """Worked example combining all four signals.

    signals: service 1.0, severity 2/3, tags 0.5, recency 0.5.
    weighted sum = 0.4*1 + 0.2*(2/3) + 0.25*0.5 + 0.15*0.5
                 = 0.4 + 0.13333 + 0.125 + 0.075 = 0.73333 (weights sum to 1).
    """
    score, breakdown = contextual_score(
        query_service="api",
        query_severity="critical",
        query_tags=["a", "b", "c"],
        cand_service="api",
        cand_severity="high",
        cand_tags=["b", "c", "d"],
        cand_created_at=_NOW - timedelta(days=30),
        weights=_DEFAULT_WEIGHTS,
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )

    # Individual signals surfaced in the breakdown.
    assert breakdown["service"] == 1.0
    assert breakdown["severity"] == pytest.approx(2 / 3)
    assert breakdown["tags"] == 0.5
    assert breakdown["recency"] == pytest.approx(0.5)

    # Combined score.
    assert score == pytest.approx(0.73333, abs=1e-5)

    # breakdown contract: exact key set, incl. the weights actually used.
    assert set(breakdown) == {"service", "severity", "tags", "recency", "weights"}
    assert breakdown["weights"] == _DEFAULT_WEIGHTS


def test_contextual_score_weight_scale_invariance() -> None:
    """Scaling every weight by the same factor must not change the score.

    The average is weight-normalised (divided by the weight sum), so weights
    ×10 yield an identical score.
    """
    common = dict(
        query_service="api",
        query_severity="critical",
        query_tags=["a", "b", "c"],
        cand_service="api",
        cand_severity="high",
        cand_tags=["b", "c", "d"],
        cand_created_at=_NOW - timedelta(days=30),
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )

    base_score, _ = contextual_score(weights=_DEFAULT_WEIGHTS, **common)
    scaled_weights = {k: v * 10 for k, v in _DEFAULT_WEIGHTS.items()}
    scaled_score, _ = contextual_score(weights=scaled_weights, **common)

    assert scaled_score == pytest.approx(base_score)
    assert scaled_score == pytest.approx(0.73333, abs=1e-5)


def test_contextual_score_zero_weights_mean_fallback() -> None:
    """All-zero weights → plain mean of the signals.

    With every signal at 1.0 the mean is 1.0 (never a divide-by-zero or 0.0).
    """
    score, breakdown = contextual_score(
        query_service="api",
        query_severity="high",
        query_tags=["a", "b"],
        cand_service="api",
        cand_severity="high",
        cand_tags=["a", "b"],
        cand_created_at=_NOW,  # age 0 → recency 1.0
        weights={"service": 0.0, "severity": 0.0, "tags": 0.0, "recency": 0.0},
        half_life_days=_HALF_LIFE,
        now=_NOW,
    )

    # Sanity: all four signals are 1.0 so the fallback mean is unambiguous.
    assert breakdown["service"] == 1.0
    assert breakdown["severity"] == 1.0
    assert breakdown["tags"] == 1.0
    assert breakdown["recency"] == 1.0
    assert score == pytest.approx(1.0)
