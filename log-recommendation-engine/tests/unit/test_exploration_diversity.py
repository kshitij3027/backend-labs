"""Unit tests for the C12 ranker guards: resolution diversity + ε-exploration.

Pure — no DB, no Redis, no embeddings, no HTTP. Everything is driven through
:func:`src.ranker.rank_candidates` (and the exposed helpers) with **explicit**
``weights``, ``now`` / ``half_life_days``, ``diversity_threshold`` and an
**injected** rng so every case is fully deterministic and independent of the
static config defaults.

The two guards under test defend against the popularity feedback loop:

* **Resolution diversity** — near-duplicate ``resolution`` texts (token-Jaccard ≥
  ``diversity_threshold``) collapse to a single visible slot, the next *distinct*
  candidate being pulled in.
* **ε-exploration** — with probability ``epsilon`` one strong-but-unproven
  (``feedback == 0``, high ``base``) candidate sitting just outside the visible page
  is promoted into the last slot and flagged ``explored``. ``epsilon == 0`` (the bare
  default) is byte-identical to the pure-exploitation C11 ranking.

Synthetic candidates are built directly as :class:`src.retrieval.Candidate` frozen
dataclasses so nothing here reaches the DB-backed retrieval query path.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest

from src.ranker import (
    RankedSuggestion,
    QueryContext,
    _pick_exploration_candidate,
    _resolution_similarity,
    _should_explore,
    rank_candidates,
)
from src.retrieval import Candidate

# Fixed reference "now" so the recency signal is deterministic across cases.
_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_HALF_LIFE = 30.0

# Explicit blend weights (mirror the config defaults 0.6 / 0.4 / 0.2). Feedback is
# weighted so a proven incumbent's votes genuinely lift its score above an
# unproven-but-relevant candidate — that gap is what exploration has to jump.
_W = {"semantic": 0.6, "contextual": 0.4, "feedback": 0.2}

# A neutral query: no service/severity/tags facets match the candidates. Every
# candidate still gets the SAME small contextual contribution from the recency
# sub-signal (all share ``created_at == _NOW`` → recency 1.0), so contextual is a
# uniform constant across candidates and the exploitation *ordering* is driven purely
# by semantic (+ feedback). That uniformity is what keeps the relative reasoning simple.
_NEUTRAL_QUERY = QueryContext(service=None, severity=None, tags=None)


class _AlwaysExploreRng:
    """Stub rng whose ``random()`` returns 0.0 → ``_should_explore`` always True."""

    def random(self) -> float:  # noqa: D401 - trivial stub
        return 0.0


class _NeverExploreRng:
    """Stub rng whose ``random()`` returns 1.0 → ``_should_explore`` never True."""

    def random(self) -> float:  # noqa: D401 - trivial stub
        return 1.0


def _candidate(
    *,
    incident_id: int,
    semantic: float,
    resolution: str | None = None,
    service: str = "svc",
    severity: str = "high",
    tags: list[str] | None = None,
) -> Candidate:
    """Build a synthetic :class:`Candidate` with a per-id distinct resolution.

    ``resolution`` defaults to a unique ``"resolution <id>"`` so, unless a test
    overrides it, no two candidates are treated as near-duplicates.
    """
    return Candidate(
        incident_id=incident_id,
        title=f"incident-{incident_id}",
        description=f"description for incident {incident_id}",
        service=service,
        severity=severity,
        tags=list(tags) if tags is not None else ["db"],
        resolution=resolution if resolution is not None else f"resolution {incident_id}",
        created_at=_NOW,
        semantic=semantic,
    )


# --------------------------------------------------------------------------- #
# 1. Forced ε-exploration — an unproven candidate below the page is surfaced
# --------------------------------------------------------------------------- #
def test_forced_exploration_surfaces_unproven_candidate() -> None:
    """With ε=1.0 and an always-fire rng, one strong-but-unproven candidate below the
    visible page is promoted into the last slot and flagged ``explored``, while a
    strong incumbent is retained.

    Setup (top_k=2). The neutral query adds a *uniform* +0.06 contextual (recency)
    term to every candidate's base, so it cancels out of the ordering; relative to
    semantic (+ feedback) the exploitation order is:

    * incident 1 — semantic 0.9, feedback +1.0  → strongest incumbent
    * incident 2 — semantic 0.8, feedback +1.0  → weaker incumbent
    * incident 3 — semantic 0.5, feedback  0.0  → unproven, ranks below the page

    Exploitation order is [1, 2, 3]; the page (top_k=2) is [1, 2]. Candidate 3 is
    unproven (feedback 0), has the highest ``base`` among the unproven-and-not-visible
    set, and sits just outside the page → exploration promotes it into slot 2,
    displacing incumbent 2. Incumbent 1 (the strongest) is retained.
    """
    cands = [
        _candidate(incident_id=1, semantic=0.9),
        _candidate(incident_id=2, semantic=0.8),
        _candidate(incident_id=3, semantic=0.5),
    ]
    feedback_scores = {1: 1.0, 2: 1.0}  # incidents 1 & 2 proven; 3 never voted.

    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores=feedback_scores,
        diversity_threshold=0.9,
        epsilon=1.0,
        rng=_AlwaysExploreRng(),
    )

    ids = [r.incident_id for r in results]
    assert len(results) == 2
    # The unproven candidate 3 was surfaced despite ranking below the page...
    assert 3 in ids, f"exploration did not surface the unproven candidate; got {ids}"
    # ...and the strongest incumbent (1) is retained.
    assert 1 in ids, f"strong incumbent 1 was dropped; got {ids}"

    explored = next(r for r in results if r.incident_id == 3)
    assert explored.explored is True
    assert explored.breakdown["explored"] is True
    # The retained incumbent is NOT flagged as explored.
    incumbent = next(r for r in results if r.incident_id == 1)
    assert incumbent.explored is False
    assert "explored" not in incumbent.breakdown


def test_forced_exploration_with_seeded_random() -> None:
    """Same promotion, but driven by a real seeded ``random.Random`` rather than a stub.

    ε=1.0 means ``_should_explore`` fires for *any* draw < 1.0 (i.e. always), so a
    plain seeded Random exercises the real ``rng.random()`` path and still surfaces the
    unproven candidate deterministically.
    """
    cands = [
        _candidate(incident_id=1, semantic=0.9),
        _candidate(incident_id=2, semantic=0.8),
        _candidate(incident_id=3, semantic=0.5),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={1: 1.0, 2: 1.0},
        diversity_threshold=0.9,
        epsilon=1.0,
        rng=random.Random(0),
    )
    explored = [r for r in results if r.explored]
    assert len(explored) == 1
    assert explored[0].incident_id == 3


# --------------------------------------------------------------------------- #
# 2. Resolution diversity — a duplicate resolution is dropped, distinct pulled in
# --------------------------------------------------------------------------- #
def test_diversity_drops_duplicate_resolution() -> None:
    """Two candidates share the SAME ``resolution`` text; ``top_k=2`` keeps only the
    higher-scoring duplicate and pulls in the distinct third candidate.

    The neutral query adds a uniform recency term to every candidate, so with no
    feedback the score order still follows semantic: [1 (0.9), 2 (0.8), 3 (0.5)].
    Candidates 1 and 2 carry the identical resolution
    ("restart the database pool"); candidate 3 is distinct. The diversity de-dup skips
    2 (near-duplicate of 1) and takes 3 instead → page is [1, 3].
    """
    dup = "restart the database pool"
    cands = [
        _candidate(incident_id=1, semantic=0.9, resolution=dup),
        _candidate(incident_id=2, semantic=0.8, resolution=dup),
        _candidate(incident_id=3, semantic=0.5, resolution="scale out the web tier"),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        diversity_threshold=0.9,
        epsilon=0.0,  # keep exploration out of this diversity-only assertion.
    )
    ids = [r.incident_id for r in results]
    assert ids == [1, 3], f"diversity did not drop the duplicate; got {ids}"
    # The dropped duplicate (2) is absent; the distinct candidate (3) is present.
    assert 2 not in ids


def test_diversity_reorders_words_still_duplicate() -> None:
    """Resolutions that are the same words in a different order are still de-duped.

    ``_resolution_similarity`` is a *set*-token Jaccard, so "raise the pool size" and
    "the pool size raise" are identity-similar (1.0) and one is dropped.
    """
    cands = [
        _candidate(incident_id=1, semantic=0.9, resolution="raise the pool size"),
        _candidate(incident_id=2, semantic=0.8, resolution="the pool size raise"),
        _candidate(incident_id=3, semantic=0.5, resolution="add read replicas"),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        diversity_threshold=0.9,
        epsilon=0.0,
    )
    assert [r.incident_id for r in results] == [1, 3]


def test_diversity_noop_for_distinct_resolutions() -> None:
    """When every resolution is distinct, diversity changes nothing (pure score order)."""
    cands = [
        _candidate(incident_id=1, semantic=0.9, resolution="alpha fix"),
        _candidate(incident_id=2, semantic=0.8, resolution="beta fix"),
        _candidate(incident_id=3, semantic=0.5, resolution="gamma fix"),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=3,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        diversity_threshold=0.9,
        epsilon=0.0,
    )
    assert [r.incident_id for r in results] == [1, 2, 3]


# --------------------------------------------------------------------------- #
# 3. epsilon == 0 → unchanged, pure-exploitation ranking (no explored flag)
# --------------------------------------------------------------------------- #
def test_epsilon_zero_is_pure_score_order_no_flag() -> None:
    """``epsilon=0.0`` yields pure score order and never sets an ``explored`` flag,
    even with an always-fire rng supplied (ε=0 short-circuits before the draw)."""
    cands = [
        _candidate(incident_id=1, semantic=0.9),
        _candidate(incident_id=2, semantic=0.8),
        _candidate(incident_id=3, semantic=0.5),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={1: 1.0, 2: 1.0},
        diversity_threshold=0.9,
        epsilon=0.0,
        rng=_AlwaysExploreRng(),  # even so, ε=0 means no exploration.
    )
    assert [r.incident_id for r in results] == [1, 2]
    assert all(r.explored is False for r in results)
    assert all("explored" not in r.breakdown for r in results)


def test_bare_default_epsilon_matches_c11_exploitation() -> None:
    """A bare call (no ``epsilon``/``rng``) is byte-identical to pure exploitation.

    The default epsilon is 0.0, so even the module-default rng is never consulted and
    the result is the plain score-ordered page.
    """
    cands = [
        _candidate(incident_id=1, semantic=0.9),
        _candidate(incident_id=2, semantic=0.8),
        _candidate(incident_id=3, semantic=0.5),
    ]
    results = rank_candidates(
        cands,
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=2,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        feedback_scores={1: 1.0, 2: 1.0},
        diversity_threshold=0.9,
    )
    assert [r.incident_id for r in results] == [1, 2]
    assert all(r.explored is False for r in results)


# --------------------------------------------------------------------------- #
# 4. breakdown gains "base"; explored breakdown gains "explored"
# --------------------------------------------------------------------------- #
def test_breakdown_carries_base() -> None:
    """Every suggestion's breakdown carries ``base`` (feedback-free semantic+contextual).

    ``base == w_sem*semantic + w_ctx*contextual`` (no feedback term). Note the neutral
    query still yields a small non-zero contextual via the *recency* sub-signal
    (``created_at == now`` → recency 1.0), so we assert ``base`` against the result's own
    ``contextual`` rather than assuming contextual is 0.
    """
    cand = _candidate(incident_id=1, semantic=0.5)
    (result,) = rank_candidates(
        [cand],
        _NEUTRAL_QUERY,
        weights=_W,
        top_k=1,
        half_life_days=_HALF_LIFE,
        now=_NOW,
        epsilon=0.0,
    )
    assert "base" in result.breakdown
    assert result.breakdown["base"] == pytest.approx(
        0.6 * result.semantic + 0.4 * result.contextual
    )
    # And base excludes any feedback contribution (feedback is 0 here anyway).
    assert result.breakdown["base"] == pytest.approx(result.score)


# --------------------------------------------------------------------------- #
# 5. Helper: _resolution_similarity
# --------------------------------------------------------------------------- #
def test_resolution_similarity_identical_is_one() -> None:
    assert _resolution_similarity("restart the pool", "restart the pool") == 1.0


def test_resolution_similarity_word_order_independent() -> None:
    assert _resolution_similarity("restart the pool", "pool the restart") == 1.0


def test_resolution_similarity_case_insensitive() -> None:
    assert _resolution_similarity("Restart The Pool", "restart the pool") == 1.0


def test_resolution_similarity_empty_is_zero() -> None:
    # Both empty → 0.0 (blank resolutions never suppress each other).
    assert _resolution_similarity("", "") == 0.0
    # One empty, one not → 0.0.
    assert _resolution_similarity("", "restart the pool") == 0.0


def test_resolution_similarity_disjoint_is_zero() -> None:
    assert _resolution_similarity("alpha beta", "gamma delta") == 0.0


def test_resolution_similarity_partial_overlap() -> None:
    # {a, b, c} vs {b, c, d} → intersection {b, c} (2), union {a,b,c,d} (4) → 0.5.
    assert _resolution_similarity("a b c", "b c d") == pytest.approx(0.5)


# --------------------------------------------------------------------------- #
# 6. Helper: _should_explore
# --------------------------------------------------------------------------- #
def test_should_explore_epsilon_zero_is_false() -> None:
    # ε=0 short-circuits to False without drawing from the rng at all.
    assert _should_explore(_AlwaysExploreRng(), 0.0) is False


def test_should_explore_fires_when_draw_below_epsilon() -> None:
    assert _should_explore(_AlwaysExploreRng(), 1.0) is True  # draw 0.0 < 1.0


def test_should_explore_suppressed_when_draw_at_or_above_epsilon() -> None:
    # draw 1.0 is NOT < epsilon 0.5 → no exploration.
    assert _should_explore(_NeverExploreRng(), 0.5) is False


# --------------------------------------------------------------------------- #
# 7. Helper: _pick_exploration_candidate
# --------------------------------------------------------------------------- #
def _ranked(
    incident_id: int, *, base: float, semantic: float, feedback: float
) -> RankedSuggestion:
    """Build a minimal :class:`RankedSuggestion` with the fields the picker reads."""
    return RankedSuggestion(
        incident_id=incident_id,
        title=f"i-{incident_id}",
        description="d",
        service="svc",
        severity="high",
        tags=["db"],
        resolution=f"res {incident_id}",
        created_at=_NOW,
        score=base + 0.2 * feedback,
        semantic=semantic,
        contextual=0.0,
        feedback=feedback,
        breakdown={"base": base},
    )


def test_pick_exploration_prefers_highest_base_among_unproven_hidden() -> None:
    """Among unproven (feedback 0) candidates NOT already visible, the highest ``base``
    is chosen; a stronger-base but already-visible one is ignored, as is a proven one."""
    visible = _ranked(1, base=0.9, semantic=0.9, feedback=0.0)  # already on the page
    hidden_strong = _ranked(2, base=0.6, semantic=0.6, feedback=0.0)  # unproven, hidden
    hidden_weak = _ranked(3, base=0.4, semantic=0.4, feedback=0.0)  # unproven, hidden
    proven_hidden = _ranked(4, base=0.8, semantic=0.8, feedback=1.0)  # proven → skip

    ranked = [visible]
    all_candidates = [visible, hidden_strong, hidden_weak, proven_hidden]

    picked = _pick_exploration_candidate(ranked, all_candidates)
    assert picked is not None
    # Highest base among {2 (0.6), 3 (0.4)} that is unproven & not visible → 2.
    assert picked.incident_id == 2


def test_pick_exploration_returns_none_when_all_unproven_visible() -> None:
    """When every unproven candidate is already visible (and the rest are proven),
    there is nothing to promote → ``None``."""
    a = _ranked(1, base=0.9, semantic=0.9, feedback=0.0)  # unproven but visible
    b = _ranked(2, base=0.8, semantic=0.8, feedback=1.0)  # proven, hidden
    picked = _pick_exploration_candidate([a], [a, b])
    assert picked is None


def test_pick_exploration_returns_none_when_no_unproven() -> None:
    """No unproven candidates at all → ``None`` (exploration is a safe no-op)."""
    a = _ranked(1, base=0.9, semantic=0.9, feedback=1.0)
    b = _ranked(2, base=0.8, semantic=0.8, feedback=1.0)
    picked = _pick_exploration_candidate([a], [a, b])
    assert picked is None
