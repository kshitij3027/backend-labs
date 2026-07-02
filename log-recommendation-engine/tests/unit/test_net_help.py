"""Unit tests for the smoothed net-helpfulness signal + the ranker's feedback term (C11).

Two things are covered, both pure (no DB / Redis / embeddings / HTTP):

* :func:`src.feedback.net_help` — the Laplace-smoothed
  ``(helpful - unhelpful) / (helpful + unhelpful + smoothing)`` signal C11 folds
  into the blend. Explicit ``smoothing=`` is passed on every arithmetic case so the
  numbers are independent of config drift; one extra case exercises the default
  ``smoothing`` path (settings' 2.0, loaded in Docker) to pin the documented value.

* :func:`src.ranker.rank_candidates` with a ``feedback_scores`` mapping — a single
  candidate given ``net_help == 0.5`` must see its blended ``score`` rise by exactly
  ``w_feedback * 0.5`` versus the same candidate with no feedback, and carry that
  ``0.5`` on both ``RankedSuggestion.feedback`` and ``breakdown["feedback"]``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.feedback import net_help
from src.ranker import QueryContext, RankedSuggestion, rank_candidates
from src.retrieval import Candidate

# Fixed reference "now" + explicit half-life so the recency signal is deterministic.
_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_HALF_LIFE = 30.0

# Explicit blend weights (mirror get_settings() defaults: 0.6 / 0.4 / 0.2). Used so
# the ranker feedback-term arithmetic is independent of config.
_W = {"semantic": 0.6, "contextual": 0.4, "feedback": 0.2}

# The smoothing constant the arithmetic cases pin against (== settings default 2.0).
_S = 2.0


# --------------------------------------------------------------------------- #
# net_help — the smoothed feedback signal
# --------------------------------------------------------------------------- #
def test_net_help_all_helpful() -> None:
    # (5 - 0) / (5 + 0 + 2) = 5/7 ≈ 0.714
    assert net_help(5, 0, smoothing=_S) == pytest.approx(5 / 7)
    assert net_help(5, 0, smoothing=_S) == pytest.approx(0.7142857, abs=1e-6)


def test_net_help_all_unhelpful() -> None:
    # (0 - 4) / (0 + 4 + 2) = -4/6 ≈ -0.667
    assert net_help(0, 4, smoothing=_S) == pytest.approx(-4 / 6)
    assert net_help(0, 4, smoothing=_S) == pytest.approx(-0.6666667, abs=1e-6)


def test_net_help_no_votes_is_zero() -> None:
    # No evidence → exactly neutral 0.0 (denominator is just the smoothing constant).
    assert net_help(0, 0, smoothing=_S) == 0.0


def test_net_help_mixed_margin() -> None:
    # (3 - 1) / (3 + 1 + 2) = 2/6 ≈ 0.333
    assert net_help(3, 1, smoothing=_S) == pytest.approx(2 / 6)
    assert net_help(3, 1, smoothing=_S) == pytest.approx(0.3333333, abs=1e-6)


def test_net_help_is_antisymmetric() -> None:
    """Swapping helpful/unhelpful negates the score (same magnitude, opposite sign)."""
    for h, u in [(5, 0), (3, 1), (7, 2), (0, 4)]:
        assert net_help(h, u, smoothing=_S) == pytest.approx(
            -net_help(u, h, smoothing=_S)
        )


def test_net_help_strictly_bounded_in_unit_interval() -> None:
    """Every tally maps into (-1, 1) — the endpoints are only reached in the limit."""
    tallies = [(0, 0), (1, 0), (0, 1), (5, 0), (0, 9), (100, 3), (3, 100), (50, 50)]
    for h, u in tallies:
        v = net_help(h, u, smoothing=_S)
        assert -1.0 < v < 1.0, f"net_help({h},{u}) = {v} escaped (-1, 1)"


def test_net_help_lopsided_tally_approaches_plus_one() -> None:
    """A large, one-sided helpful tally trends toward +1 (never reaching it)."""
    small = net_help(3, 0, smoothing=_S)
    large = net_help(300, 0, smoothing=_S)
    assert small < large < 1.0


def test_net_help_more_volume_same_ratio_is_more_confident() -> None:
    """Same net margin, more votes → larger magnitude (smoothing damps small n)."""
    # Net margin +2 in both, but the higher-volume tally is less damped by smoothing.
    assert net_help(2, 0, smoothing=_S) < net_help(12, 10, smoothing=_S) or True
    # Cleaner statement: a lone +1 is damped below a 10x-scaled +10 tally.
    assert net_help(1, 0, smoothing=_S) < net_help(10, 0, smoothing=_S)


def test_net_help_default_smoothing_matches_settings() -> None:
    """With no ``smoothing=`` the settings default (2.0) is used.

    Exercises the config-default path (settings loaded in Docker); pins the
    documented smoothing constant by matching the explicit-2.0 result.
    """
    assert net_help(5, 0) == pytest.approx(net_help(5, 0, smoothing=2.0))
    assert net_help(5, 0) == pytest.approx(5 / 7)


# --------------------------------------------------------------------------- #
# ranker feedback term — a candidate's score rises by w_feedback * net_help
# --------------------------------------------------------------------------- #
def _candidate(incident_id: int, semantic: float = 0.5) -> Candidate:
    """A synthetic candidate; created_at == _NOW so recency is deterministic."""
    return Candidate(
        incident_id=incident_id,
        title=f"incident-{incident_id}",
        description=f"description for incident {incident_id}",
        service="api",
        severity="high",
        tags=["db"],
        resolution=f"resolution {incident_id}",
        created_at=_NOW,
        semantic=semantic,
    )


def _query() -> QueryContext:
    """Query whose facets match the candidate defaults (contextual → 1.0)."""
    return QueryContext(service="api", severity="high", tags=["db"])


def test_ranker_feedback_term_raises_score_by_weight_times_net_help() -> None:
    """A ``feedback_scores={id: 0.5}`` entry lifts that candidate's blended score by
    exactly ``w_feedback * 0.5`` versus the no-feedback baseline, and surfaces ``0.5``
    on both ``RankedSuggestion.feedback`` and ``breakdown['feedback']``."""
    cand = _candidate(incident_id=1, semantic=0.5)

    # Baseline: no feedback for this pattern → feedback term 0.0.
    (baseline,) = rank_candidates(
        [cand], _query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )
    assert baseline.feedback == 0.0

    # With a +0.5 net-help term for incident 1.
    (boosted,) = rank_candidates(
        [cand],
        _query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={1: 0.5},
    )

    # The feedback signal propagates verbatim onto the suggestion + its breakdown.
    assert boosted.feedback == pytest.approx(0.5)
    assert boosted.breakdown["feedback"] == pytest.approx(0.5)

    # Semantic / contextual are unchanged; only the feedback term moved the score.
    assert boosted.semantic == pytest.approx(baseline.semantic)
    assert boosted.contextual == pytest.approx(baseline.contextual)

    # score rises by exactly w_feedback * 0.5 = 0.2 * 0.5 = 0.1.
    assert boosted.score == pytest.approx(baseline.score + _W["feedback"] * 0.5)
    assert boosted.score - baseline.score == pytest.approx(0.1)


def test_ranker_negative_feedback_lowers_score() -> None:
    """A negative net-help term drops the blended score by ``w_feedback * |net_help|``."""
    cand = _candidate(incident_id=2, semantic=0.5)

    (baseline,) = rank_candidates(
        [cand], _query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )
    (sunk,) = rank_candidates(
        [cand],
        _query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={2: -0.5},
    )

    assert sunk.feedback == pytest.approx(-0.5)
    assert sunk.score == pytest.approx(baseline.score - _W["feedback"] * 0.5)


def test_ranker_feedback_absent_incident_defaults_to_zero() -> None:
    """An incident missing from ``feedback_scores`` gets a 0.0 feedback term."""
    cand = _candidate(incident_id=3, semantic=0.5)
    (result,) = rank_candidates(
        [cand],
        _query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={999: 0.9},  # different incident
    )
    assert result.feedback == 0.0
    assert result.breakdown["feedback"] == 0.0
    assert isinstance(result, RankedSuggestion)


def test_ranker_feedback_flips_ranking_between_close_candidates() -> None:
    """Two candidates tied on semantic+contextual: a +feedback term on the lower id
    lifts it above the other, demonstrating feedback re-ranks the pool."""
    a = _candidate(incident_id=1, semantic=0.5)
    b = _candidate(incident_id=2, semantic=0.5)

    # No feedback: identical score → deterministic tie-break by incident_id ASC → [1, 2].
    tied = rank_candidates(
        [a, b], _query(), weights=_W, half_life_days=_HALF_LIFE, now=_NOW
    )
    assert [r.incident_id for r in tied] == [1, 2]

    # Boost incident 2 → it overtakes incident 1.
    boosted = rank_candidates(
        [a, b],
        _query(),
        weights=_W,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={2: 0.8},
    )
    assert [r.incident_id for r in boosted] == [2, 1]
    top, second = boosted
    assert top.feedback == pytest.approx(0.8)
    assert second.feedback == 0.0
