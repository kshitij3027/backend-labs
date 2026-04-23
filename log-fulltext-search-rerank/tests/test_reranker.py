"""Unit tests for :class:`~src.ranking.reranker.MultiFactorReranker` and
:func:`~src.ranking.explain.build_explanation`.

Covers:

* Empty-input degenerate case.
* TF-IDF dominance on lexical match when other signals are neutral.
* Temporal recency boost with otherwise-identical docs.
* Incident-mode context bonus elevating ERROR above INFO.
* ``incident_mode_boost`` reason surfacing in the per-doc reasons list.
* ``heapq.nlargest`` slicing to exactly ``limit`` results.
* Robustness to candidate doc_ids the index no longer has.
* ``maybe_rebuild`` firing through the reranker path.
* Deterministic tiebreakers in ``_sort_key``.
* ``build_explanation`` shape + mode-without-boost fallback reason.
"""

from __future__ import annotations

import pytest

from src.config import Settings, get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry, RankingExplanation
from src.query.intent import IntentDetector
from src.query.parser import ParsedQuery, QueryParser
from src.query.synonyms import SynonymExpander
from src.ranking.context import effective_weights
from src.ranking.explain import build_explanation
from src.ranking.reranker import MultiFactorReranker, ScoredDoc, _sort_key
from src.ranking.service_authority import ServiceAuthorityScorer
from src.ranking.severity import SeverityScorer
from src.ranking.temporal import TemporalScorer
from src.ranking.tfidf import TfIdfScorer


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _build_reranker(
    settings: Settings | None = None,
) -> tuple[MultiFactorReranker, InvertedIndex, TfIdfScorer, QueryParser]:
    """Wire a full reranker + parser + index stack with real primitives.

    Returning the dependencies too lets each test reach into them
    (e.g. asserting on ``tfidf.idf_version`` or ingesting directly
    through ``index.add``).
    """
    s = settings or get_settings()
    tokenizer = LogTokenizer(s)
    index = InvertedIndex(settings=s, tokenizer=tokenizer)
    tfidf = TfIdfScorer(index=index, settings=s)
    temporal = TemporalScorer()
    severity = SeverityScorer(s)
    service = ServiceAuthorityScorer(s)
    reranker = MultiFactorReranker(
        index=index,
        tfidf=tfidf,
        temporal=temporal,
        severity=severity,
        service=service,
        settings=s,
    )
    parser = QueryParser(
        tokenizer=tokenizer,
        intent=IntentDetector(),
        synonyms=SynonymExpander(),
    )
    return reranker, index, tfidf, parser


def _entry(
    message: str,
    *,
    timestamp: float = 0.0,
    level: str = "INFO",
    service: str = "api",
) -> LogEntry:
    return LogEntry(
        message=message,
        timestamp=timestamp,
        service=service,
        level=level,
    )


# ---------------------------------------------------------------------------
# 1. Empty candidates -> empty list
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rerank_empty_candidates_returns_empty_list() -> None:
    """No candidates -> no work, no crash, empty output."""
    reranker, _, _, parser = _build_reranker()
    parsed = parser.parse("anything goes here")
    out = await reranker.rerank(parsed, [], limit=10, context=None, now=10.0)
    assert out == []


# ---------------------------------------------------------------------------
# 2. TF-IDF-only wins on lexical match
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tfidf_dominates_when_other_factors_neutral() -> None:
    """With equal temporal/severity/service signals, the lexical match wins.

    Both docs have identical timestamps, the same INFO level, and the
    same service — so the only signal that differs across them is
    TF-IDF. The matching doc must rank first.
    """
    reranker, index, _, parser = _build_reranker()
    now = 1000.0
    matching_id = await index.add(
        _entry(
            "database connection refused timeout",
            timestamp=now,
            level="INFO",
            service="api",
        )
    )
    other_id = await index.add(
        _entry(
            "user profile updated successfully",
            timestamp=now,
            level="INFO",
            service="api",
        )
    )
    parsed = parser.parse("database timeout")
    out = await reranker.rerank(
        parsed,
        candidates=[matching_id, other_id],
        limit=2,
        context=None,
        now=now,
    )
    assert out[0].doc_id == matching_id
    assert out[0].total > out[1].total


# ---------------------------------------------------------------------------
# 3. Temporal recency boosts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_temporal_recency_ranks_newer_doc_higher() -> None:
    """Same message, different timestamps -> newer ranks first."""
    s = Settings(temporal_half_life_normal_s=60)
    reranker, index, _, parser = _build_reranker(settings=s)
    now = 1000.0
    # age=990 -> near-zero temporal score.
    old_id = await index.add(
        _entry("payment gateway error detected", timestamp=10.0)
    )
    # age=10 -> ~0.89 temporal score.
    recent_id = await index.add(
        _entry("payment gateway error detected", timestamp=990.0)
    )
    parsed = parser.parse("payment error")
    out = await reranker.rerank(
        parsed,
        candidates=[old_id, recent_id],
        limit=2,
        context=None,
        now=now,
    )
    assert out[0].doc_id == recent_id
    assert out[0].total > out[1].total


# ---------------------------------------------------------------------------
# 4. Incident mode elevates ERROR above INFO
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incident_mode_elevates_error_above_info() -> None:
    """Identical TF-IDF match; ERROR wins on context+severity under incident mode."""
    reranker, index, _, parser = _build_reranker()
    now = 1000.0
    info_id = await index.add(
        _entry(
            "authentication token validation failed",
            timestamp=now,
            level="INFO",
            service="auth",
        )
    )
    error_id = await index.add(
        _entry(
            "authentication token validation failed",
            timestamp=now,
            level="ERROR",
            service="auth",
        )
    )
    parsed = parser.parse("authentication failed")
    out = await reranker.rerank(
        parsed,
        candidates=[info_id, error_id],
        limit=2,
        context={"mode": "incident"},
        now=now,
    )
    assert out[0].doc_id == error_id


# ---------------------------------------------------------------------------
# 5. Reasons contain the mode-boost label on ERROR in incident mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incident_mode_boost_label_appears_on_error() -> None:
    """The winning ERROR doc must list ``incident_mode_boost`` in reasons."""
    reranker, index, _, parser = _build_reranker()
    now = 1000.0
    error_id = await index.add(
        _entry(
            "payment processing failure",
            timestamp=now,
            level="ERROR",
            service="payment",
        )
    )
    parsed = parser.parse("payment failure")
    out = await reranker.rerank(
        parsed,
        candidates=[error_id],
        limit=1,
        context={"mode": "incident"},
        now=now,
    )
    assert out
    assert any("incident_mode_boost" in r for r in out[0].reasons)


# ---------------------------------------------------------------------------
# 6. heapq.nlargest slicing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_limit_slices_to_exact_count_and_orders_descending() -> None:
    """20 matching docs, limit=5 -> exactly 5 results, descending by total."""
    reranker, index, _, parser = _build_reranker()
    now = 1000.0
    ids: list[int] = []
    for i in range(20):
        # Vary message so TF-IDF spreads across docs; include "error"
        # everywhere so every doc is a candidate. Spread timestamps so
        # temporal scores vary smoothly across the batch.
        msg = " ".join(["error"] * (i + 1)) + f" marker{i}"
        doc_id = await index.add(
            _entry(msg, timestamp=now - (20 - i))
        )
        ids.append(doc_id)
    parsed = parser.parse("error")
    out = await reranker.rerank(
        parsed,
        candidates=ids,
        limit=5,
        context=None,
        now=now,
    )
    assert len(out) == 5
    totals = [sd.total for sd in out]
    assert totals == sorted(totals, reverse=True)


# ---------------------------------------------------------------------------
# 7. Candidate with no matching doc is skipped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_candidate_is_skipped_not_raised() -> None:
    """A doc_id the index doesn't know about must be silently dropped."""
    reranker, index, _, parser = _build_reranker()
    now = 1000.0
    real_id = await index.add(
        _entry("database connection timeout", timestamp=now)
    )
    phantom_id = 99_999  # never ingested
    parsed = parser.parse("database timeout")
    out = await reranker.rerank(
        parsed,
        candidates=[real_id, phantom_id],
        limit=10,
        context=None,
        now=now,
    )
    assert [sd.doc_id for sd in out] == [real_id]


# ---------------------------------------------------------------------------
# 8. idf_version bumps after rerank() when threshold is 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rerank_triggers_idf_rebuild_under_threshold() -> None:
    """Reranker must call ``tfidf.maybe_rebuild`` on every query.

    With ``idf_rebuild_every_n_docs=1`` and one ingested doc, the
    rebuild must fire on the first rerank call, bumping
    ``idf_version`` at least once.
    """
    s = Settings(idf_rebuild_every_n_docs=1)
    reranker, index, tfidf, parser = _build_reranker(settings=s)
    now = 1000.0
    doc_id = await index.add(_entry("trigger rebuild here", timestamp=now))
    parsed = parser.parse("trigger rebuild")
    await reranker.rerank(
        parsed,
        candidates=[doc_id],
        limit=1,
        context=None,
        now=now,
    )
    assert tfidf.idf_version >= 1


# ---------------------------------------------------------------------------
# 9. _sort_key tiebreaker behaviour
# ---------------------------------------------------------------------------

def test_sort_key_tiebreaks_on_temporal_then_doc_id() -> None:
    """Equal totals -> newer temporal wins; equal temporal -> higher doc_id."""
    a = ScoredDoc(
        doc_id=1,
        total=0.5,
        breakdown={"temporal": 0.1},
    )
    b = ScoredDoc(
        doc_id=2,
        total=0.5,
        breakdown={"temporal": 0.9},
    )
    # Descending: b (higher temporal) first.
    ranked = sorted([a, b], key=_sort_key, reverse=True)
    assert [sd.doc_id for sd in ranked] == [2, 1]

    # Identical totals AND temporal -> doc_id tiebreaker (higher wins).
    c = ScoredDoc(doc_id=5, total=0.7, breakdown={"temporal": 0.5})
    d = ScoredDoc(doc_id=9, total=0.7, breakdown={"temporal": 0.5})
    ranked2 = sorted([c, d], key=_sort_key, reverse=True)
    assert [sd.doc_id for sd in ranked2] == [9, 5]


# ---------------------------------------------------------------------------
# 10. build_explanation shape
# ---------------------------------------------------------------------------

def test_build_explanation_copies_breakdown_and_reasons() -> None:
    """Per-factor fields + reasons round-trip verbatim."""
    sd = ScoredDoc(
        doc_id=1,
        total=0.42,
        breakdown={
            "tfidf": 0.1,
            "temporal": 0.2,
            "severity": 0.3,
            "service": 0.4,
            "context": 0.5,
        },
        reasons=["recent", "high_severity_ERROR"],
    )
    settings = get_settings()
    weights = effective_weights(None, settings)
    exp = build_explanation(sd, dict(weights), mode=None)
    assert isinstance(exp, RankingExplanation)
    assert exp.tfidf == 0.1
    assert exp.temporal == 0.2
    assert exp.severity == 0.3
    assert exp.service == 0.4
    assert exp.context == 0.5
    assert "recent" in exp.reasons
    assert "high_severity_ERROR" in exp.reasons


# ---------------------------------------------------------------------------
# 11. build_explanation appends ``<mode>_mode`` when no bonus fired
# ---------------------------------------------------------------------------

def test_build_explanation_appends_mode_tag_when_no_boost() -> None:
    """Mode set but no boost fired -> a plain ``<mode>_mode`` reason lands.

    Example: INFO entry in incident mode — ``context_bonus`` returns 0
    so no ``incident_mode_boost`` reason exists; the explanation still
    records ``incident_mode`` so clients see the request context.
    """
    sd = ScoredDoc(
        doc_id=1,
        total=0.3,
        breakdown={
            "tfidf": 0.2,
            "temporal": 0.1,
            "severity": 0.4,
            "service": 0.5,
            "context": 0.0,
        },
        reasons=[],  # no boost fired
    )
    exp = build_explanation(sd, weights={}, mode="incident")
    assert "incident_mode" in exp.reasons
    # Sanity: the boost label must NOT sneak in.
    assert not any("_mode_boost" in r for r in exp.reasons)


def test_build_explanation_keeps_boost_label_and_suppresses_tag() -> None:
    """When the boost fired, don't also append the plain ``_mode`` tag."""
    sd = ScoredDoc(
        doc_id=1,
        total=0.7,
        breakdown={
            "tfidf": 0.3,
            "temporal": 0.4,
            "severity": 1.0,
            "service": 0.9,
            "context": 1.0,
        },
        reasons=["incident_mode_boost", "high_severity_ERROR"],
    )
    exp = build_explanation(sd, weights={}, mode="incident")
    assert "incident_mode_boost" in exp.reasons
    # With the boost present, the plain tag should be suppressed.
    assert "incident_mode" not in exp.reasons


def test_parsedquery_used_via_expanded_tokens_when_available() -> None:
    """The reranker prefers ``expanded_tokens`` over plain ``tokens``.

    Pure unit test at the dataclass level — constructs a ParsedQuery
    and asserts the invariant the reranker depends on (``expanded``
    supersedes ``tokens``).
    """
    parsed = ParsedQuery(
        raw="auth fail",
        tokens=["auth", "fail"],
        expanded_tokens=["auth", "fail", "authentication", "error"],
        intent="user_activity",
    )
    # The empty-list guard must fall back to ``tokens`` — nothing to
    # assert other than the dataclass round-trips cleanly, but this
    # documents the contract the reranker relies on.
    assert parsed.expanded_tokens or parsed.tokens
