"""Redis-backed shard for a single index node.

All keys are namespaced with the node_id so multiple nodes can share a
single Redis instance without collision.
"""

from __future__ import annotations

from redis.asyncio import Redis

from shared.models import TermTF


class NodeShard:
    """Per-node inverted-index shard backed by Redis."""

    def __init__(self, node_id: str, redis: Redis) -> None:
        self.node_id = node_id
        self.redis = redis

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------
    def _postings_key(self, term: str) -> str:
        return f"node:{self.node_id}:postings:{term}"

    def _tf_key(self, doc_id: str, term: str) -> str:
        return f"node:{self.node_id}:tf:{doc_id}:{term}"

    def _terms_key(self) -> str:
        return f"node:{self.node_id}:meta:terms"

    def _docs_key(self) -> str:
        return f"node:{self.node_id}:meta:docs"

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------
    async def index_terms(self, doc_id: str, terms: list[TermTF]) -> int:
        """Index ``terms`` for ``doc_id``; returns number of terms indexed."""
        if not terms:
            return 0
        terms_key = self._terms_key()
        docs_key = self._docs_key()
        pipe = self.redis.pipeline(transaction=False)
        for t in terms:
            pipe.sadd(self._postings_key(t.term), doc_id)
            pipe.sadd(terms_key, t.term)
            pipe.set(self._tf_key(doc_id, t.term), int(t.tf))
            pipe.sadd(docs_key, doc_id)
        await pipe.execute()
        return len(terms)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------
    async def get_posting_list(self, term: str) -> tuple[list[str], int]:
        """Return ``(sorted doc_ids, doc_frequency)`` for ``term``."""
        doc_ids = await self.redis.smembers(self._postings_key(term))
        sorted_ids = sorted(doc_ids)
        return sorted_ids, len(sorted_ids)

    async def get_term_frequency(self, doc_id: str, term: str) -> int:
        """Return the stored term frequency for ``(doc_id, term)`` or 0."""
        raw = await self.redis.get(self._tf_key(doc_id, term))
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    async def get_postings_batch(
        self, terms: list[str]
    ) -> list[tuple[str, list[str], int]]:
        """Batch posting-list lookup. Missing terms return ``(term, [], 0)``."""
        if not terms:
            return []
        pipe = self.redis.pipeline(transaction=False)
        for term in terms:
            pipe.smembers(self._postings_key(term))
        raw_results = await pipe.execute()
        out: list[tuple[str, list[str], int]] = []
        for term, members in zip(terms, raw_results):
            members_list = sorted(members) if members else []
            out.append((term, members_list, len(members_list)))
        return out

    async def stats(self) -> dict:
        """Return ``{"term_count": int, "document_count": int}``."""
        pipe = self.redis.pipeline(transaction=False)
        pipe.scard(self._terms_key())
        pipe.scard(self._docs_key())
        term_count, document_count = await pipe.execute()
        return {
            "term_count": int(term_count or 0),
            "document_count": int(document_count or 0),
        }

    async def ping(self) -> bool:
        """Return True if Redis responds to PING."""
        try:
            return bool(await self.redis.ping())
        except Exception:
            return False
