"""Unit tests for :class:`~src.ranking.tfidf.TfIdfScorer`.

Covers the fresh-state invariants, monotonicity w.r.t. repeated-term
matches, idf monotonicity w.r.t. document frequency, and the cache
rebuild policy (version, time, no-op).
"""

from __future__ import annotations

import math

import pytest

from src.config import Settings, get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry
from src.ranking.tfidf import TfIdfScorer


def _fresh_index_and_scorer(settings: Settings | None = None) -> tuple[InvertedIndex, TfIdfScorer]:
    s = settings or get_settings()
    tokenizer = LogTokenizer(s)
    index = InvertedIndex(settings=s, tokenizer=tokenizer)
    return index, TfIdfScorer(index=index, settings=s)


def _entry(message: str, level: str = "INFO", service: str = "api") -> LogEntry:
    return LogEntry(message=message, timestamp=0.0, service=service, level=level)


# ---------------------------------------------------------------------------
# Scoring behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_tokens_returns_zero() -> None:
    """A fresh scorer fed no query tokens must collapse to 0.0."""
    _, scorer = _fresh_index_and_scorer()
    assert scorer.score(doc_id=0, tokens=[]) == 0.0


@pytest.mark.asyncio
async def test_single_token_match_gives_positive_score() -> None:
    """Matching doc scores > 0; non-matching doc scores exactly 0."""
    index, scorer = _fresh_index_and_scorer()
    matching_id = await index.add(_entry("database connection error timeout"))
    other_id = await index.add(_entry("user login success"))
    match_score = scorer.score(doc_id=matching_id, tokens=["error"])
    other_score = scorer.score(doc_id=other_id, tokens=["error"])
    assert match_score > 0.0
    assert other_score == 0.0


@pytest.mark.asyncio
async def test_repeated_term_doc_scores_higher() -> None:
    """A doc mentioning the term 3x outranks one mentioning it once."""
    index, scorer = _fresh_index_and_scorer()
    heavy = await index.add(_entry("error error error in payment flow"))
    light = await index.add(_entry("another error here"))
    heavy_score = scorer.score(doc_id=heavy, tokens=["error"])
    light_score = scorer.score(doc_id=light, tokens=["error"])
    assert heavy_score > light_score


@pytest.mark.asyncio
async def test_common_token_has_lower_idf_than_rare_token() -> None:
    """IDF is monotonically non-increasing in document frequency."""
    index, scorer = _fresh_index_and_scorer()
    # "common" appears in 10 docs, "rare" in 1.
    for i in range(10):
        await index.add(_entry(f"common payload number {i} here"))
    await index.add(_entry("rare occurrence spotted"))
    assert scorer.idf("common") < scorer.idf("rare")


# ---------------------------------------------------------------------------
# Cache rebuild
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_maybe_rebuild_populates_cache_when_version_threshold_met() -> None:
    """With threshold=1, a single add + rebuild populates the idf_cache."""
    s = Settings(idf_rebuild_every_n_docs=1)
    index, scorer = _fresh_index_and_scorer(settings=s)
    await index.add(_entry("error in payment"))
    scorer.maybe_rebuild()
    assert scorer.idf_cache
    assert scorer.idf_version == 1


@pytest.mark.asyncio
async def test_maybe_rebuild_no_op_when_version_unchanged() -> None:
    """Rebuild is idempotent when version hasn't advanced."""
    s = Settings(idf_rebuild_every_n_docs=1, idf_rebuild_every_s=0.0)
    index, scorer = _fresh_index_and_scorer(settings=s)
    await index.add(_entry("first entry with content"))
    scorer.maybe_rebuild()
    v1 = scorer.idf_version
    assert v1 == 1
    # Second call with no intervening write should be a no-op.
    scorer.maybe_rebuild()
    assert scorer.idf_version == v1


@pytest.mark.asyncio
async def test_maybe_rebuild_time_based_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Time threshold fires the rebuild even if version delta is tiny."""
    # Huge n_docs threshold so only the time rule can fire.
    s = Settings(idf_rebuild_every_n_docs=10_000, idf_rebuild_every_s=0.5)
    index, scorer = _fresh_index_and_scorer(settings=s)
    await index.add(_entry("first entry"))

    # Monkeypatch time.monotonic *inside the tfidf module* so the
    # rebuild sees the skew immediately.
    fake_now = {"t": 100.0}

    def fake_monotonic() -> float:
        return fake_now["t"]

    import src.ranking.tfidf as tfidf_mod

    monkeypatch.setattr(tfidf_mod.time, "monotonic", fake_monotonic)

    # First rebuild to prime last_built_time.
    scorer.maybe_rebuild()
    assert scorer.idf_version == 1

    # Advance time past the threshold and ingest 1 more doc so
    # `versions_since > 0` — then the time rule should fire.
    fake_now["t"] += 10.0
    await index.add(_entry("second entry"))
    scorer.maybe_rebuild()
    assert scorer.idf_version == 2


@pytest.mark.asyncio
async def test_score_uses_idf_cache_when_present() -> None:
    """If idf_cache is set for a token, score uses the cached value."""
    index, scorer = _fresh_index_and_scorer()
    doc_id = await index.add(_entry("error error in payment"))
    # Manually plant a very large idf for "error".
    scorer.idf_cache["error"] = 99.0
    score_with_planted = scorer.score(doc_id=doc_id, tokens=["error"])
    # Compute the expected value directly to assert the cache won.
    tf = index.token_frequency("error", doc_id)
    doc_len = index.doc_length(doc_id) or 1
    expected = (tf / doc_len) * 99.0
    assert math.isclose(score_with_planted, expected, rel_tol=1e-9)


@pytest.mark.asyncio
async def test_idf_smoothing_never_negative() -> None:
    """``log((N+1)/(df+1)) + 1`` is always >= 1 for any valid df."""
    index, scorer = _fresh_index_and_scorer()
    for _ in range(5):
        await index.add(_entry("payment error"))
    # df("payment") == 5, N == 5 -> log(6/6)+1 == 1.0
    assert scorer.idf("payment") >= 1.0
