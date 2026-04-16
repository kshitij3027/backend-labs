"""Scatter-gather query planner for the coordinator.

Responsibilities:
  * Tokenize the search query.
  * Route each term to its owning node via the consistent hash ring.
  * Fan out ``/search_terms`` requests to those nodes in parallel.
  * Merge posting lists with AND/OR semantics.
  * Hydrate doc bodies from the shared ``docs:{doc_id}`` Redis hashes.
  * Score with a simple TF-IDF-ish rank and return a ``SearchResponse``.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Any, Literal

import httpx

from coordinator.retry import retry_async
from shared.models import (
    PostingEntry,
    SearchRequest,
    SearchResponse,
    SearchResultItem,
)


class QueryPlanner:
    """Coordinator-side planner for distributed search."""

    def __init__(
        self,
        registry: Any,
        ring: Any,
        tokenizer: Any,
        client: httpx.AsyncClient | None,
        shared_redis: Any,
        cache: Any = None,
        retry_count: int = 3,
        retry_base_delay: float = 0.1,
    ) -> None:
        self.registry = registry
        self.ring = ring
        self.tokenizer = tokenizer
        self.client = client
        self.redis = shared_redis
        self.cache = cache
        self.retry_count = retry_count
        self.retry_base_delay = retry_base_delay

    # ------------------------------------------------------------------
    # Doc hydration
    # ------------------------------------------------------------------
    async def fetch_doc_bodies(self, doc_ids: list[str]) -> dict[str, dict]:
        """Fetch ``docs:{doc_id}`` hashes for each doc_id. Missing docs omitted."""
        if not doc_ids or self.redis is None:
            return {}
        pipe = self.redis.pipeline(transaction=False)
        for did in doc_ids:
            pipe.hgetall(f"docs:{did}")
        raw = await pipe.execute()
        out: dict[str, dict] = {}
        for did, fields in zip(doc_ids, raw):
            if not fields:
                continue
            content = fields.get("content", "")
            meta_raw = fields.get("metadata", "")
            try:
                metadata = json.loads(meta_raw) if meta_raw else {}
            except (ValueError, TypeError):
                metadata = {}
            out[did] = {"content": content, "metadata": metadata}
        return out

    # ------------------------------------------------------------------
    # Scatter
    # ------------------------------------------------------------------
    async def scatter(
        self, term_groups: dict[str, list[str]]
    ) -> tuple[dict[str, list[PostingEntry]], list[str], list[str]]:
        """Fan out ``/search_terms`` to each owning node in parallel.

        Returns a tuple ``(postings_by_term, queried_nodes, failed_nodes)``.
        ``postings_by_term`` maps each term to the list of PostingEntry
        responses that mention it (typically 1 per term in this sharding
        scheme, but multiple are merged if encountered).
        """
        postings_by_term: dict[str, list[PostingEntry]] = {}
        queried: list[str] = []
        failed: list[str] = []

        if not term_groups:
            return postings_by_term, queried, failed

        node_ids = list(term_groups.keys())

        async def _call(node_id: str, terms: list[str]) -> Any:
            base = self.registry.url_for(node_id)
            if base is None:
                raise RuntimeError(f"unknown node_id: {node_id}")
            url = base.rstrip("/") + "/search_terms"

            async def _do() -> Any:
                resp = await self.client.post(url, json={"terms": terms})
                # Treat 5xx as retryable failures.
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"server error {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                return resp

            try:
                return await retry_async(
                    _do,
                    attempts=self.retry_count,
                    base_delay=self.retry_base_delay,
                )
            except Exception as e:
                return e

        tasks = [_call(nid, term_groups[nid]) for nid in node_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for nid, res in zip(node_ids, results):
            if isinstance(res, BaseException):
                failed.append(nid)
                continue
            try:
                if res.status_code != 200:
                    failed.append(nid)
                    continue
                body = res.json()
            except Exception:
                failed.append(nid)
                continue
            queried.append(nid)
            for p in body.get("postings", []):
                try:
                    entry = PostingEntry(**p)
                except Exception:
                    continue
                postings_by_term.setdefault(entry.term, []).append(entry)
        return postings_by_term, queried, failed

    # ------------------------------------------------------------------
    # Merge / score
    # ------------------------------------------------------------------
    @staticmethod
    def _flatten(
        postings_by_term: dict[str, list[PostingEntry]] | dict[str, PostingEntry]
    ) -> dict[str, PostingEntry]:
        """Reduce list-valued posting map (one entry per responding node) to a
        single PostingEntry per term by unioning doc_ids."""
        flat: dict[str, PostingEntry] = {}
        for term, value in postings_by_term.items():
            if isinstance(value, PostingEntry):
                flat[term] = value
                continue
            if not value:
                continue
            doc_ids: set[str] = set()
            for entry in value:
                doc_ids.update(entry.doc_ids)
            merged_ids = sorted(doc_ids)
            flat[term] = PostingEntry(
                term=term, doc_ids=merged_ids, doc_frequency=len(merged_ids)
            )
        return flat

    def merge(
        self,
        postings_by_term: dict[str, Any],
        op: Literal["AND", "OR"],
        query_terms: list[str] | None = None,
    ) -> set[str]:
        """Merge postings across terms with AND intersection or OR union.

        ``query_terms`` — when provided, enforces that every query term must
        exist in ``postings_by_term`` for AND (a missing term -> empty set).
        If omitted, the merge is taken over the keys actually present.
        """
        flat = self._flatten(postings_by_term)
        if op == "AND":
            keys = query_terms if query_terms is not None else list(flat.keys())
            if not keys:
                return set()
            result: set[str] | None = None
            for term in keys:
                entry = flat.get(term)
                if entry is None or not entry.doc_ids:
                    return set()
                ids = set(entry.doc_ids)
                result = ids if result is None else (result & ids)
                if not result:
                    return set()
            return result or set()
        # OR
        keys = query_terms if query_terms is not None else list(flat.keys())
        out: set[str] = set()
        for term in keys:
            entry = flat.get(term)
            if entry is None:
                continue
            out.update(entry.doc_ids)
        return out

    def score(
        self,
        doc_ids: set[str],
        postings_by_term: dict[str, Any],
        query_terms: list[str],
        total_docs_hint: int = 1000,
    ) -> list[tuple[str, float]]:
        """Simple TF-IDF-ish score: sum of log(N/df) for each matching term."""
        flat = self._flatten(postings_by_term)
        # Precompute per-term IDF + posting sets
        term_weights: list[tuple[set[str], float]] = []
        for term in query_terms:
            entry = flat.get(term)
            if entry is None:
                continue
            df = max(1, entry.doc_frequency or len(entry.doc_ids))
            idf = math.log(max(1.0, total_docs_hint / df) + 1.0)
            term_weights.append((set(entry.doc_ids), idf))

        scored: list[tuple[str, float]] = []
        for did in doc_ids:
            s = 0.0
            for ids, idf in term_weights:
                if did in ids:
                    s += idf
            scored.append((did, s))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    # ------------------------------------------------------------------
    # Top-level search
    # ------------------------------------------------------------------
    async def search(self, req: SearchRequest) -> SearchResponse:
        t0 = time.perf_counter()

        # Cache lookup (only successful previous responses are cached)
        if self.cache is not None:
            cached = self.cache.get(req)
            if cached is not None:
                return cached.model_copy(
                    update={
                        "cached": True,
                        "search_time_ms": (time.perf_counter() - t0) * 1000,
                    }
                )

        terms = self.tokenizer.tokenize(req.query) if self.tokenizer else []
        if not terms:
            return SearchResponse(
                documents=[],
                total_results=0,
                search_time_ms=(time.perf_counter() - t0) * 1000,
                nodes_queried=[],
                failed_nodes=[],
                routing_ms=0.0,
                scatter_ms=0.0,
                merge_ms=0.0,
            )

        # Routing
        t_route = time.perf_counter()
        term_groups = self.ring.get_nodes_for_terms(terms)
        routing_ms = (time.perf_counter() - t_route) * 1000

        # Scatter
        t_scatter = time.perf_counter()
        postings_by_term, queried, failed = await self.scatter(term_groups)
        scatter_ms = (time.perf_counter() - t_scatter) * 1000

        # Merge + score + hydrate
        t_merge = time.perf_counter()
        doc_set = self.merge(postings_by_term, req.op, query_terms=terms)
        scored = self.score(doc_set, postings_by_term, terms)
        limited = scored[: req.limit]
        limited_ids = [d for d, _ in limited]
        bodies = await self.fetch_doc_bodies(limited_ids)
        items: list[SearchResultItem] = []
        for did, s in limited:
            body = bodies.get(did, {})
            items.append(
                SearchResultItem(
                    doc_id=did,
                    content=body.get("content", ""),
                    score=float(s),
                    metadata=body.get("metadata", {}),
                )
            )
        merge_ms = (time.perf_counter() - t_merge) * 1000

        search_time_ms = (time.perf_counter() - t0) * 1000
        response = SearchResponse(
            documents=items,
            total_results=len(scored),
            search_time_ms=search_time_ms,
            nodes_queried=sorted(queried),
            failed_nodes=sorted(failed),
            routing_ms=routing_ms,
            scatter_ms=scatter_ms,
            merge_ms=merge_ms,
            cached=False,
        )
        # Only cache successful, complete responses.
        if self.cache is not None and not failed:
            self.cache.put(req, response)
        return response
