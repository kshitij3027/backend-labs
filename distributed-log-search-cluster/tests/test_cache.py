"""Tests for the coordinator ResultCache."""

from __future__ import annotations

from coordinator.cache import ResultCache
from shared.models import SearchRequest, SearchResponse


def _resp(n: int = 0) -> SearchResponse:
    return SearchResponse(
        documents=[],
        total_results=n,
        search_time_ms=1.0,
        nodes_queried=[],
        failed_nodes=[],
    )


def test_cache_hit_returns_same_object() -> None:
    cache = ResultCache(size=10, ttl=60)
    req = SearchRequest(query="error timeout", op="AND", limit=10)
    resp = _resp(5)
    cache.put(req, resp)
    got = cache.get(req)
    assert got is resp


def test_cache_miss_returns_none() -> None:
    cache = ResultCache(size=10, ttl=60)
    req = SearchRequest(query="nope", op="AND", limit=10)
    assert cache.get(req) is None


def test_cache_key_ignores_case_and_whitespace() -> None:
    cache = ResultCache(size=10, ttl=60)
    r1 = SearchRequest(query="  Error  ", op="AND", limit=10)
    r2 = SearchRequest(query="error", op="AND", limit=10)
    assert ResultCache.key_for(r1) == ResultCache.key_for(r2)
    cache.put(r1, _resp(1))
    assert cache.get(r2) is not None


def test_cache_respects_op() -> None:
    r_and = SearchRequest(query="error", op="AND", limit=10)
    r_or = SearchRequest(query="error", op="OR", limit=10)
    assert ResultCache.key_for(r_and) != ResultCache.key_for(r_or)


def test_cache_respects_limit() -> None:
    r1 = SearchRequest(query="error", op="AND", limit=10)
    r2 = SearchRequest(query="error", op="AND", limit=20)
    assert ResultCache.key_for(r1) != ResultCache.key_for(r2)
