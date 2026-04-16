"""Coordinator-side document indexing: write body to shared Redis, fan out
term groups to owning nodes."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from shared.models import DocumentInput


class Indexer:
    """Fan-out indexer driven by the consistent hash ring."""

    def __init__(
        self,
        registry: Any,
        ring: Any,
        tokenizer: Any,
        client: httpx.AsyncClient,
        shared_redis: Any,
    ) -> None:
        self.registry = registry
        self.ring = ring
        self.tokenizer = tokenizer
        self.client = client
        self.redis = shared_redis

    async def index_document(self, doc: DocumentInput) -> dict:
        # 1) Tokenize with positions → per-term TF (count of positions).
        positions = self.tokenizer.tokenize_with_positions(doc.content)
        tf_by_term: dict[str, int] = {t: len(p) for t, p in positions.items()}

        # 2) Store doc body in shared Redis hash docs:{doc_id}.
        await self.redis.hset(
            f"docs:{doc.doc_id}",
            mapping={
                "content": doc.content,
                "metadata": json.dumps(doc.metadata or {}),
            },
        )

        if not tf_by_term:
            return {"doc_id": doc.doc_id, "indexed_terms": 0, "nodes_written": []}

        # 3) Group terms by owning node.
        groups = self.ring.get_nodes_for_terms(tf_by_term.keys())

        # 4) POST /index_terms to each owning node.
        async def _push(node_id: str, terms: list[str]) -> tuple[str, bool]:
            base = self.registry.url_for(node_id)
            if base is None:
                return node_id, False
            url = base.rstrip("/") + "/index_terms"
            payload = {
                "doc_id": doc.doc_id,
                "terms": [
                    {"term": t, "tf": int(tf_by_term[t])} for t in terms
                ],
            }
            try:
                resp = await self.client.post(url, json=payload)
            except Exception:
                return node_id, False
            return node_id, resp.status_code == 200

        node_ids = list(groups.keys())
        tasks = [_push(nid, groups[nid]) for nid in node_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        nodes_written: list[str] = []
        for res in results:
            if isinstance(res, BaseException):
                continue
            nid, ok = res
            if ok:
                nodes_written.append(nid)

        return {
            "doc_id": doc.doc_id,
            "indexed_terms": len(tf_by_term),
            "nodes_written": sorted(nodes_written),
        }
