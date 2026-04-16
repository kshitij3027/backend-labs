"""Coordinator FastAPI tests using respx for node mocking + fakeredis for docs."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import httpx
import pytest
import respx
from fakeredis import aioredis
from fastapi.testclient import TestClient

from coordinator.cluster import ClusterRegistry
from coordinator.indexer import Indexer
from coordinator.main import create_app
from coordinator.planner import QueryPlanner
from shared.config import CoordinatorSettings
from shared.hash_ring import ConsistentHashRing
from shared.tokenizer import LogTokenizer


NODE_URLS = {
    "node-1": "http://node-1:8101",
    "node-2": "http://node-2:8102",
    "node-3": "http://node-3:8103",
    "node-4": "http://node-4:8104",
}


@pytest.fixture
def test_env():
    """Spin up a coordinator app wired to fakeredis + a real httpx client
    (intercepted by respx in each test)."""
    settings = CoordinatorSettings(
        coordinator_port=8000,
        node_urls=",".join(f"{k}={v}" for k, v in NODE_URLS.items()),
        virtual_nodes=100,
        redis_host="localhost",
        redis_port=6379,
    )
    app = create_app(settings)
    redis = aioredis.FakeRedis(decode_responses=True)
    client = httpx.AsyncClient(timeout=httpx.Timeout(5.0))

    registry = ClusterRegistry(NODE_URLS)
    ring = ConsistentHashRing(virtual_nodes=100)
    for nid in registry.nodes():
        ring.add_node(nid)
    tokenizer = LogTokenizer()

    @asynccontextmanager
    async def test_lifespan(a):
        a.state.registry = registry
        a.state.ring = ring
        a.state.tokenizer = tokenizer
        a.state.client = client
        a.state.redis = redis
        a.state.planner = QueryPlanner(registry, ring, tokenizer, client, redis)
        a.state.indexer = Indexer(registry, ring, tokenizer, client, redis)
        a.state.settings = settings
        try:
            yield
        finally:
            await client.aclose()
            await redis.aclose()

    app.router.lifespan_context = test_lifespan

    with TestClient(app) as tc:
        yield {
            "client": tc,
            "redis": redis,
            "ring": ring,
            "tokenizer": tokenizer,
        }


def test_health_with_all_nodes_healthy(test_env):
    tc = test_env["client"]
    with respx.mock(assert_all_called=False) as mock:
        for nid, url in NODE_URLS.items():
            mock.get(f"{url}/health").mock(
                return_value=httpx.Response(
                    200, json={"status": "healthy", "node_id": nid}
                )
            )
        r = tc.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["healthy_nodes"] == 4
    assert body["total_nodes"] == 4
    assert body["status"] == "healthy"
    assert all(body["nodes"].values())


def test_index_and_search_roundtrip(test_env):
    tc = test_env["client"]
    ring = test_env["ring"]
    tokenizer = test_env["tokenizer"]

    content = "critical database error timeout at service"
    terms = tokenizer.tokenize(content)
    assert terms, "tokenizer should produce terms for a real sentence"
    groups = ring.get_nodes_for_terms(terms)

    with respx.mock(assert_all_called=False) as mock:
        # Mock /index_terms on every node — returns 200
        for url in NODE_URLS.values():
            mock.post(f"{url}/index_terms").mock(
                return_value=httpx.Response(200, json={"indexed": 1})
            )

        r = tc.post(
            "/index",
            json={"doc_id": "d1", "content": content, "metadata": {"lvl": 1}},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["doc_id"] == "d1"
        assert body["indexed_terms"] == len(terms)
        # Every group node should show up in nodes_written.
        assert set(body["nodes_written"]) == set(groups.keys())

    # Pick a term and mock /search_terms on its owning node.
    query_term = terms[0]
    owner = ring.get_node(query_term)
    owner_url = NODE_URLS[owner]

    with respx.mock(assert_all_called=False) as mock:
        mock.post(f"{owner_url}/search_terms").mock(
            return_value=httpx.Response(
                200,
                json={
                    "node_id": owner,
                    "postings": [
                        {
                            "term": query_term,
                            "doc_ids": ["d1"],
                            "doc_frequency": 1,
                        }
                    ],
                },
            )
        )

        r = tc.post(
            "/search",
            json={"query": query_term, "op": "AND", "limit": 10},
        )
        assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_results"] == 1
    assert len(body["documents"]) == 1
    doc = body["documents"][0]
    assert doc["doc_id"] == "d1"
    # Hydration should pull the content we stored in fakeredis.
    assert doc["content"] == content
    assert doc["metadata"] == {"lvl": 1}
    assert owner in body["nodes_queried"]


def test_empty_query_returns_empty(test_env):
    tc = test_env["client"]
    # "the" is a stop word → tokenizer returns []
    r = tc.post("/search", json={"query": "the", "op": "AND", "limit": 10})
    assert r.status_code == 200
    body = r.json()
    assert body["total_results"] == 0
    assert body["documents"] == []
    assert body["nodes_queried"] == []
    assert body["failed_nodes"] == []
