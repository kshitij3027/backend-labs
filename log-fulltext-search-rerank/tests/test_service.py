"""Unit tests for :class:`~src.service.SearchService`.

Drives the service directly (no HTTP) so failures point at the
pipeline wiring, not at FastAPI routing. Each test builds a fresh
service instance with a fresh in-memory index so state never leaks
between cases.
"""

from __future__ import annotations

import pytest

from src.cache.query_cache import QueryCache
from src.config import get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.index.trie import PrefixTrie
from src.models import LogEntry, SearchRequest
from src.query.intent import IntentDetector
from src.query.parser import QueryParser
from src.query.synonyms import SynonymExpander
from src.ranking.reranker import MultiFactorReranker
from src.ranking.service_authority import ServiceAuthorityScorer
from src.ranking.severity import SeverityScorer
from src.ranking.temporal import TemporalScorer
from src.ranking.tfidf import TfIdfScorer
from src.service import SearchService


def _build_service() -> SearchService:
    """Wire up every dependency with fresh instances for one test.

    Mirrors :func:`src.main.build_app` but exposed as a plain factory
    so individual tests don't have to reach into the FastAPI app.
    """
    settings = get_settings()
    tokenizer = LogTokenizer(settings)
    index = InvertedIndex(settings=settings, tokenizer=tokenizer)
    intent = IntentDetector()
    synonyms = SynonymExpander(path=settings.synonyms_path, use_wordnet=False)
    parser = QueryParser(tokenizer, intent, synonyms)
    tfidf = TfIdfScorer(index, settings)
    temporal = TemporalScorer()
    severity = SeverityScorer(settings)
    service_auth = ServiceAuthorityScorer(settings)
    reranker = MultiFactorReranker(
        index=index,
        tfidf=tfidf,
        temporal=temporal,
        severity=severity,
        service=service_auth,
        settings=settings,
    )
    cache = QueryCache(max_size=settings.query_cache_size)
    trie = PrefixTrie()
    return SearchService(
        index=index,
        parser=parser,
        reranker=reranker,
        tfidf=tfidf,
        cache=cache,
        trie=trie,
        settings=settings,
    )


# ---------------------------------------------------------------------------
# Empty-corpus behaviour
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_on_empty_index_returns_empty_response() -> None:
    """No docs -> no results, but the response shape is still complete."""
    service = _build_service()
    resp = await service.search(SearchRequest(query="anything", limit=5))
    assert resp.results == []
    assert resp.total_hits == 0
    assert resp.ranked_hits == 0
    assert resp.execution_time_ms >= 0


# ---------------------------------------------------------------------------
# Ranked retrieval
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_returns_ordered_hits() -> None:
    """Ingest two distinct docs and query for the one that should win."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication error on user login", timestamp=1_700_000_000.0, level="ERROR", service="auth")
    )
    await service._index.add(
        LogEntry(message="payment timeout on checkout", timestamp=1_700_000_000.0, level="WARN", service="payment")
    )
    resp = await service.search(SearchRequest(query="authentication error", limit=5))
    assert resp.ranked_hits >= 1
    top = resp.results[0]
    # The top hit should be the authentication doc because it matches
    # both tokens of the query.
    assert "authentication" in top.log_entry.lower()


@pytest.mark.asyncio
async def test_search_latency_is_reasonable_on_small_corpus() -> None:
    """Sanity: a query over a tiny corpus stays well under CI tolerance."""
    service = _build_service()
    for i in range(5):
        await service._index.add(
            LogEntry(
                message=f"authentication failed for user {i}",
                timestamp=1_700_000_000.0 + i,
                level="ERROR",
                service="auth",
            )
        )
    resp = await service.search(SearchRequest(query="authentication", limit=10))
    # 200ms is very generous for 5 docs — this catches catastrophic
    # regressions (infinite loops, accidental full-corpus scans)
    # rather than real perf shifts.
    assert resp.execution_time_ms < 200


# ---------------------------------------------------------------------------
# Cache semantics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_repeat_query_hits_cache() -> None:
    """Two identical requests: the second should hit the LRU cache."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication error", timestamp=1.0, level="ERROR")
    )
    await service.search(SearchRequest(query="authentication error", limit=5))
    resp2 = await service.search(SearchRequest(query="authentication error", limit=5))
    # Cache hit counter advanced.
    assert service._cache.hits == 1
    # Cache-hit responses are near-zero ms on a trivial corpus.
    assert resp2.execution_time_ms < 5.0


@pytest.mark.asyncio
async def test_different_mode_misses_cache() -> None:
    """The cache key includes ``mode`` so a different mode is a miss."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication error", timestamp=1.0, level="ERROR")
    )
    await service.search(SearchRequest(query="authentication error", limit=5))
    await service.search(
        SearchRequest(
            query="authentication error",
            limit=5,
            context={"mode": "incident"},
        )
    )
    # Two different cache keys -> two misses.
    assert service._cache.misses == 2
    assert service._cache.hits == 0


@pytest.mark.asyncio
async def test_ingest_invalidates_cache_via_version_bump() -> None:
    """A post-query ingest bumps ``index.version`` so the next get misses."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication error", timestamp=1.0, level="ERROR")
    )
    await service.search(SearchRequest(query="authentication error", limit=5))
    # Ingest bumps index.version, so the same query has a new cache key.
    await service._index.add(
        LogEntry(message="authentication error again", timestamp=2.0, level="ERROR")
    )
    await service.search(SearchRequest(query="authentication error", limit=5))
    assert service._cache.misses == 2
    assert service._cache.hits == 0


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_reflects_corpus_and_cache_state() -> None:
    """After ingesting docs and running searches the stats look sane."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication error", timestamp=1.0, level="ERROR")
    )
    await service._index.add(
        LogEntry(message="database timeout", timestamp=2.0, level="WARN")
    )
    await service._index.add(
        LogEntry(message="payment success", timestamp=3.0, level="INFO")
    )
    await service.search(SearchRequest(query="authentication", limit=5))
    await service.search(SearchRequest(query="authentication", limit=5))
    stats = service.stats()
    assert stats.total_docs == 3
    assert stats.unique_tokens > 0
    assert 0.0 <= stats.cache_hit_ratio <= 1.0
    assert stats.p95_latency_ms >= 0.0


# ---------------------------------------------------------------------------
# Suggest
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_suggest_returns_suggestions_response() -> None:
    """``suggest`` returns a populated :class:`SuggestionsResponse`."""
    service = _build_service()
    await service._index.add(
        LogEntry(message="authentication failure", timestamp=1.0)
    )
    resp = service.suggest("auth", limit=5)
    assert isinstance(resp.suggestions, list)
    # At least one suggestion starts with the prefix.
    assert any(s.startswith("auth") for s in resp.suggestions)
