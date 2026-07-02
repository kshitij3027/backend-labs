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

Robustness against the popularity feedback loop (C12)
-----------------------------------------------------
Two guards keep the ranker from collapsing onto a handful of over-voted fixes:

* **Resolution diversity** — when assembling the final top-K we skip a candidate
  whose ``resolution`` is a near-duplicate (token-Jaccard ≥ ``diversity_threshold``)
  of one already selected, pulling the next *distinct* candidate instead. The
  page therefore shows varied fixes, not N copies of the same resolution. This
  step is fully deterministic.
* **ε-exploration** — with probability ``epsilon`` we promote ONE strong-but-unproven
  candidate (high ``base = w_sem*semantic + w_ctx*contextual`` but ``feedback == 0``,
  i.e. never voted) that would otherwise sit just outside the visible page into a
  slot, marking it ``breakdown["explored"] = True``. This gives fresh-but-relevant
  resolutions a chance to earn feedback instead of being permanently buried by the
  incumbents' accumulated votes. The randomness is driven by an **injectable**
  :class:`random.Random`, so tests force / suppress exploration deterministically;
  with ``epsilon == 0`` (or an rng that never triggers) the output is byte-identical
  to the pure-exploitation C11 ranking.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from src import contextual

if TYPE_CHECKING:  # typing-only; avoids importing the DB-backed retrieval module at runtime
    from src.retrieval import Candidate

# Module-level default RNG for ε-exploration. Tests inject their own seeded
# ``random.Random`` for determinism; production uses this shared, unseeded instance.
_DEFAULT_RNG = random.Random()


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
    #: True when this suggestion was surfaced by ε-exploration (a strong-but-unproven
    #: candidate promoted into the page rather than earned by score). Mirrored in
    #: ``breakdown["explored"]`` so it is observable both structurally and in the wire
    #: breakdown; always ``False`` for exploitation (score-ranked) results.
    explored: bool = False


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


def _resolution_similarity(a: str, b: str) -> float:
    """Token-Jaccard similarity between two resolution strings, in ``[0, 1]``.

    Each resolution is lower-cased, stripped and split on whitespace into a *set*
    of word tokens; the similarity is ``|A ∩ B| / |A ∪ B|`` (0 when both are empty,
    which is treated as "not similar" so two blank resolutions never suppress each
    other). Order- and duplicate-independent, cheap, and deterministic — this is the
    measure the diversity de-dup uses to detect that two suggestions carry the *same*
    fix worded slightly differently.
    """
    tokens_a = {t for t in a.lower().split() if t}
    tokens_b = {t for t in b.lower().split() if t}
    if not tokens_a and not tokens_b:
        return 0.0
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    intersection = tokens_a & tokens_b
    return len(intersection) / len(union)


def _default_diversity_threshold() -> float:
    """Return the configured resolution-diversity threshold (read lazily)."""
    from src.config import get_settings

    return float(get_settings().diversity_threshold)


def _default_epsilon() -> float:
    """Return the default ε-exploration probability for a bare ``rank_candidates`` call.

    This is deliberately ``0.0`` (pure exploitation), **not** the config value: a
    call that does not explicitly opt into exploration must be deterministic and
    byte-identical to the C11 ranker — otherwise every caller (and unit test) that
    omits ``epsilon`` would randomly reorder ~``epsilon_explore`` of the time. The
    live exploration probability from :func:`src.config.get_settings` /
    :func:`src.runtime_config.get_effective_config` is applied by the recommendation
    service, which passes it in explicitly. Pass ``epsilon=`` to opt in directly.
    """
    return 0.0


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
    diversity_threshold: float | None = None,
    epsilon: float | None = None,
    rng: random.Random | None = None,
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
    ``semantic`` descending then ``incident_id`` ascending for a stable, reproducible
    order. Before truncating to ``top_k`` (default ``settings.top_k``) two C12 guards
    against the popularity feedback loop are applied:

    * **Resolution diversity** — a candidate whose ``resolution`` is a near-duplicate
      (token-Jaccard ≥ ``diversity_threshold``, default ``settings.diversity_threshold``)
      of one already selected is skipped, and the next *distinct* candidate is pulled
      in its place. Deterministic; the page therefore shows varied fixes.
    * **ε-exploration** — with probability ``epsilon`` one strong-but-unproven
      candidate (high ``base`` = semantic+contextual but ``feedback == 0``) sitting
      just outside the diversity page is promoted into the last visible slot and
      marked ``explored``. Driven by the injectable ``rng`` (default a module-level
      :class:`random.Random`) so tests are deterministic. ``epsilon`` **defaults to
      ``0.0`` (opt-in)** here — a bare call is pure exploitation, byte-identical to the
      C11 ranking; the recommendation service passes the live
      ``settings.epsilon_explore`` (via the effective runtime config) in explicitly.
      ``epsilon == 0`` — or an rng that never triggers — is a no-op.

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

        # ``base`` is the exploitation signal *without* feedback (semantic+contextual
        # only). Exploration ranks unproven candidates by this so a never-voted but
        # strongly-relevant fix can be surfaced despite its 0 feedback term.
        base = (
            blend_weights["semantic"] * semantic
            + blend_weights["contextual"] * contextual_val
        )
        score = blended_score(
            semantic, contextual_val, feedback, weights=blend_weights
        )

        breakdown = {
            "semantic": semantic,
            "contextual": contextual_val,
            "feedback": feedback,
            "base": base,
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
    threshold = (
        diversity_threshold
        if diversity_threshold is not None
        else _default_diversity_threshold()
    )
    eps = epsilon if epsilon is not None else _default_epsilon()
    active_rng = rng if rng is not None else _DEFAULT_RNG

    # Diversity de-dup: assemble the top-`limit` by pulling distinct resolutions from
    # the score-ordered list, skipping near-duplicates of anything already chosen.
    ranked = _apply_diversity(scored, limit, threshold)

    # ε-exploration: with probability `eps`, swap the weakest visible slot for one
    # strong-but-unproven (feedback == 0) candidate just outside the page.
    if _should_explore(active_rng, eps):
        candidate = _pick_exploration_candidate(ranked, scored)
        if candidate is not None:
            ranked = _promote_exploration_candidate(ranked, candidate, limit)

    return ranked


# --------------------------------------------------------------------------- #
# C12: diversity de-dup + ε-exploration helpers
# --------------------------------------------------------------------------- #
def _apply_diversity(
    scored: list[RankedSuggestion], limit: int, threshold: float
) -> list[RankedSuggestion]:
    """Take the best ``limit`` suggestions, skipping near-duplicate resolutions.

    Walks the already score-sorted ``scored`` list and greedily selects each
    candidate whose ``resolution`` is *not* a near-duplicate (token-Jaccard ≥
    ``threshold``) of any resolution already selected, pulling the next distinct one
    when a duplicate is skipped. Deterministic. If fewer than ``limit`` distinct
    resolutions exist the (shorter) distinct list is returned as-is — we never pad the
    page back up with the duplicates we just dropped.
    """
    selected: list[RankedSuggestion] = []
    for cand in scored:
        if len(selected) >= limit:
            break
        if any(
            _resolution_similarity(cand.resolution, chosen.resolution) >= threshold
            for chosen in selected
        ):
            continue  # near-duplicate resolution already on the page — skip it.
        selected.append(cand)
    return selected


def _should_explore(rng: random.Random, epsilon: float) -> bool:
    """Return ``True`` with probability ``epsilon`` using the injected ``rng``.

    ``epsilon <= 0`` short-circuits to ``False`` (no RNG draw at all — so pure
    exploitation is bit-for-bit reproducible and never consumes randomness);
    otherwise draws ``rng.random()`` (uniform ``[0, 1)``) and compares. Isolating the
    single draw here makes exploration deterministic under a seeded/stub rng in tests.
    """
    if epsilon <= 0.0:
        return False
    return rng.random() < epsilon


def _pick_exploration_candidate(
    ranked: list[RankedSuggestion],
    all_candidates: list[RankedSuggestion],
) -> RankedSuggestion | None:
    """Choose one strong-but-unproven candidate to promote, or ``None`` if none fit.

    An "unproven" candidate has ``feedback == 0`` (never voted). We only consider
    those **not already visible** in ``ranked`` — promoting something already on the
    page is a no-op. Among the eligible ones we pick the strongest by ``base`` (the
    feedback-free semantic+contextual signal), tie-broken by ``semantic`` DESC then
    ``incident_id`` ASC to stay deterministic. Returns ``None`` when every unproven
    candidate is already shown (or there are none), making exploration a safe no-op.
    """
    visible_ids = {s.incident_id for s in ranked}
    eligible = [
        s
        for s in all_candidates
        if s.feedback == 0.0 and s.incident_id not in visible_ids
    ]
    if not eligible:
        return None
    # Highest base first; deterministic tie-break mirrors the main ranking order.
    eligible.sort(
        key=lambda s: (-s.breakdown.get("base", 0.0), -s.semantic, s.incident_id)
    )
    return eligible[0]


def _promote_exploration_candidate(
    ranked: list[RankedSuggestion],
    candidate: RankedSuggestion,
    limit: int,
) -> list[RankedSuggestion]:
    """Return ``ranked`` with ``candidate`` occupying the last visible slot, marked.

    The promoted candidate is flagged (``explored=True`` and
    ``breakdown["explored"]=True``) so the promotion is observable downstream, and it
    replaces the weakest current slot: if the page is already full it displaces the
    last entry, otherwise it is appended. The rest of the page keeps its score order;
    only the explored candidate is moved. A fresh ``RankedSuggestion`` is built (the
    dataclass is frozen) with a shallow-copied breakdown so the original is untouched.
    """
    marked_breakdown = dict(candidate.breakdown)
    marked_breakdown["explored"] = True
    explored = RankedSuggestion(
        incident_id=candidate.incident_id,
        title=candidate.title,
        description=candidate.description,
        service=candidate.service,
        severity=candidate.severity,
        tags=list(candidate.tags or []),
        resolution=candidate.resolution,
        created_at=candidate.created_at,
        score=candidate.score,
        semantic=candidate.semantic,
        contextual=candidate.contextual,
        feedback=candidate.feedback,
        breakdown=marked_breakdown,
        explored=True,
    )
    if len(ranked) >= limit:
        # Full page: displace the weakest (last) exploitation slot.
        return ranked[: limit - 1] + [explored]
    # Room to spare: append without dropping any proven suggestion.
    return ranked + [explored]


def _default_top_k() -> int:
    """Return the configured default ``top_k`` (read lazily to stay import-light)."""
    from src.config import get_settings

    return get_settings().top_k


__all__ = [
    "QueryContext",
    "RankedSuggestion",
    "blended_score",
    "rank_candidates",
    "_resolution_similarity",
    "_should_explore",
    "_pick_exploration_candidate",
]
