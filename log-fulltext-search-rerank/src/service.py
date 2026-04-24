"""The SearchService facade stitches the search pipeline together.

Pipeline:
  1. QueryParser.parse(raw) -> ParsedQuery (intent + tokens + synonyms)
  2. QueryCache lookup on (normalized_query, mode, limit, index_version)
  3. Cache miss -> InvertedIndex.retrieve_candidates(expanded_tokens)
  4. MultiFactorReranker.rerank(...) -> list[ScoredDoc]
  5. build_explanation(sd, weights, mode) for each result
  6. Wrap into SearchResponse (with query, intent, expanded_terms,
     results, total_hits, ranked_hits, execution_time_ms)
  7. Cache the response keyed on the tuple above
  8. Record latency on the cache for stats reporting

All CPU-heavy work flows through the reranker's asyncio.to_thread,
keeping the event loop responsive under load.
"""

from __future__ import annotations

import time

from src.cache.query_cache import QueryCache
from src.config import Settings
from src.index.inverted_index import InvertedIndex
from src.index.trie import PrefixTrie
from src.models import (
    SearchRequest,
    SearchResponse,
    SearchResult,
    StatsResponse,
    SuggestionsResponse,
)
from src.query.parser import QueryParser
from src.ranking.context import effective_weights
from src.ranking.explain import build_explanation
from src.ranking.reranker import MultiFactorReranker
from src.ranking.tfidf import TfIdfScorer


class SearchService:
    """Compose parser + retriever + reranker + cache into one surface.

    The service is constructed once in :func:`src.main.build_app` and
    stashed on ``app.state.search_service`` so every request handler
    reaches for the same instance. That shared-ness matters because
    the :class:`~src.cache.query_cache.QueryCache` and the
    :class:`~src.ranking.tfidf.TfIdfScorer` both own per-instance
    state (the LRU ring and the ``idf_cache``) that would drift if
    duplicated across handlers.
    """

    def __init__(
        self,
        index: InvertedIndex,
        parser: QueryParser,
        reranker: MultiFactorReranker,
        tfidf: TfIdfScorer,
        cache: QueryCache,
        trie: PrefixTrie,
        settings: Settings,
    ) -> None:
        self._index = index
        self._parser = parser
        self._reranker = reranker
        self._tfidf = tfidf
        self._cache = cache
        self._trie = trie
        self._settings = settings

    async def search(self, req: SearchRequest) -> SearchResponse:
        """Run the full search pipeline for ``req`` and return a response.

        On a cache hit the stored response is returned with a freshly
        computed ``execution_time_ms`` — callers care about the cost
        of *this* call, not the cost of the original miss.
        """
        t_start = time.perf_counter()
        mode = (req.context or {}).get("mode")
        # Normalise the query surface so ``"Error "`` and ``"error"``
        # collapse to one cache entry. The tokenizer already
        # lowercases, but the key needs to agree before the parser
        # runs so a cache hit can skip the parse entirely.
        normalized = req.query.strip().lower()
        # Include ``limit`` so different page sizes do not share
        # truncated result lists. Include ``index.version`` so any
        # successful ingest silently expires stale entries.
        cache_key = (normalized, mode, req.limit, self._index.version)
        cached = self._cache.get(cache_key)
        if cached is not None:
            elapsed_ms = (time.perf_counter() - t_start) * 1000.0
            self._cache.record_latency(elapsed_ms)
            # Return a copy so mutating the cached response elsewhere
            # can't corrupt the entry sitting in the LRU.
            return cached.model_copy(update={"execution_time_ms": round(elapsed_ms, 3)})

        parsed = self._parser.parse(req.query)
        candidates = self._index.retrieve_candidates(
            parsed.expanded_tokens or parsed.tokens,
            top_k=self._settings.candidate_top_k,
        )
        now = time.time()
        scored = await self._reranker.rerank(
            parsed=parsed,
            candidates=candidates,
            limit=req.limit,
            context=req.context,
            now=now,
        )
        weights = dict(effective_weights(mode, self._settings))
        results: list[SearchResult] = []
        for sd in scored:
            entry = self._index.doc(sd.doc_id)
            if entry is None:
                # Defensive: the reranker already filters missing
                # docs, but a race with eviction is conceivable.
                continue
            results.append(
                SearchResult(
                    log_entry=entry.message,
                    timestamp=entry.timestamp,
                    service=entry.service,
                    level=entry.level,
                    score=sd.total,
                    ranking_explanation=build_explanation(sd, weights, mode),
                )
            )
        elapsed_ms = (time.perf_counter() - t_start) * 1000.0
        resp = SearchResponse(
            query=req.query,
            intent=parsed.intent,
            expanded_terms=parsed.expanded_tokens,
            results=results,
            total_hits=len(candidates),
            ranked_hits=len(results),
            execution_time_ms=round(elapsed_ms, 3),
        )
        self._cache.put(cache_key, resp)
        self._cache.record_latency(elapsed_ms)
        return resp

    def suggest(self, prefix: str, limit: int = 10) -> SuggestionsResponse:
        """Return autocomplete suggestions for ``prefix`` via the trie.

        Mirrors the lazy-rebuild pattern the old suggestions route
        used directly: when the stored ``_indexed_version`` marker
        diverges from the live ``index.version`` the trie is wiped
        and re-populated from the current vocabulary. The route layer
        (:mod:`src.api.routes_search`) now delegates here so the
        rebuild rule lives in one place.
        """
        if getattr(self._trie, "_indexed_version", -1) != self._index.version:
            self._trie.clear()
            for tok in self._index.unique_tokens():
                self._trie.insert(tok, freq=self._index.doc_frequency(tok))
            self._trie._indexed_version = self._index.version
        return SuggestionsResponse(
            suggestions=self._trie.suggest(prefix.lower(), limit=limit)
        )

    def stats(self) -> StatsResponse:
        """Return the combined index + cache + latency snapshot.

        Reads are cheap — :meth:`InvertedIndex.stats` and the cache
        counters are pure Python dict lookups — so it's safe to call
        this on every dashboard refresh without rate-limiting.
        """
        idx_stats = self._index.stats()
        return StatsResponse(
            total_docs=int(idx_stats.get("total_docs", 0)),
            unique_tokens=int(idx_stats.get("unique_tokens", 0)),
            index_version=int(idx_stats.get("version", 0)),
            idf_version=int(self._tfidf.idf_version),
            cache_hit_ratio=float(self._cache.hit_ratio),
            p95_latency_ms=float(self._cache.p95_latency_ms()),
        )


__all__ = ("SearchService",)
