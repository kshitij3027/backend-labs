"""Unit tests for :class:`~src.ranking.temporal.TemporalScorer`.

Covers the boundary values (age=0, age=half_life, future ts,
half_life=0) and the monotonicity invariant the reranker relies on
to order by recency.
"""

from __future__ import annotations

import math

from src.ranking.temporal import TemporalScorer


def test_score_at_now_equals_one() -> None:
    """Age 0 -> weight 1.0 (new is as-good-as-it-gets)."""
    scorer = TemporalScorer()
    assert scorer.score(ts=1000.0, now=1000.0, half_life_s=60) == 1.0


def test_score_at_half_life_is_one_half() -> None:
    """By definition: at one half-life of age, the score halves."""
    scorer = TemporalScorer()
    s = scorer.score(ts=0.0, now=120.0, half_life_s=120)
    assert math.isclose(s, 0.5, rel_tol=1e-9)


def test_score_monotonically_decreases_in_age() -> None:
    """Score is a strictly decreasing function of age at fixed half-life."""
    scorer = TemporalScorer()
    half = 120
    scores = [scorer.score(ts=0.0, now=age, half_life_s=half) for age in (0, 60, 120, 240)]
    for a, b in zip(scores, scores[1:]):
        assert a > b


def test_future_timestamp_clamps_to_one() -> None:
    """Clock skew -> ts > now. Age clamped at 0 so score == 1.0."""
    scorer = TemporalScorer()
    assert scorer.score(ts=2000.0, now=1000.0, half_life_s=60) == 1.0


def test_zero_half_life_returns_zero() -> None:
    """Degenerate half-life collapses the curve to 0, not 1."""
    scorer = TemporalScorer()
    assert scorer.score(ts=0.0, now=100.0, half_life_s=0) == 0.0


def test_negative_half_life_returns_zero() -> None:
    """Defensive: a negative half-life is treated like zero."""
    scorer = TemporalScorer()
    assert scorer.score(ts=0.0, now=100.0, half_life_s=-1) == 0.0
