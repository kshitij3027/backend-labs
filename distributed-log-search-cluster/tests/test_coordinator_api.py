"""Coordinator FastAPI tests using respx for node mocking + fakeredis for docs."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import httpx
import pytest
import respx
from fakeredis import aioredis
from fastapi.testclient import TestClient

from coordinator.cache import ResultCache
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
        cache = ResultCache(size=128, ttl=60)
        a.state.cache = cache
        a.state.planner = QueryPlanner(
            registry,
            ring,
            tokenizer,
            client,
            redis,
            cache=cache,
            retry_count=1,
            retry_base_delay=0.0,
        )
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


def test_partial_failure_returns_results_and_marks_failed_node(test_env):
    tc = test_env["client"]
    ring = test_env["ring"]
    redis = test_env["redis"]

    # Seed a doc body so hydration finds content. fakeredis is a single
    # in-memory store; seeding from a one-shot loop is safe because the
    # TestClient thread only touches redis inside its own request handlers.
    import asyncio as _asyncio

    async def _seed():
        await redis.hset(
            "docs:d7",
            mapping={"content": "alpha beta gamma", "metadata": json.dumps({})},
        )

    loop = _asyncio.new_event_loop()
    try:
        loop.run_until_complete(_seed())
    finally:
        loop.close()

    # Build a multi-term query whose terms land on >=2 distinct owning nodes.
    candidates = [
        "alpha",
        "beta",
        "gamma",
        "delta",
        "epsilon",
        "zeta",
        "eta",
        "theta",
    ]
    owners = {t: ring.get_node(t) for t in candidates}
    distinct = sorted(set(owners.values()))
    assert len(distinct) >= 2, f"need 2+ owners, got {owners}"
    failing_node = distinct[0]
    # Choose two terms routed to different nodes.
    term_for_failing = next(t for t, o in owners.items() if o == failing_node)
    term_for_healthy = next(t for t, o in owners.items() if o != failing_node)
    healthy_node = owners[term_for_healthy]

    with respx.mock(assert_all_called=False) as mock:
        # Failing node: returns 500 every time (retry_count=1, so no retries).
        mock.post(f"{NODE_URLS[failing_node]}/search_terms").mock(
            return_value=httpx.Response(500, json={"detail": "boom"})
        )
        # Healthy node: returns a valid posting for d7.
        mock.post(f"{NODE_URLS[healthy_node]}/search_terms").mock(
            return_value=httpx.Response(
                200,
                json={
                    "node_id": healthy_node,
                    "postings": [
                        {
                            "term": term_for_healthy,
                            "doc_ids": ["d7"],
                            "doc_frequency": 1,
                        }
                    ],
                },
            )
        )

        r = tc.post(
            "/search",
            json={
                "query": f"{term_for_failing} {term_for_healthy}",
                "op": "OR",
                "limit": 10,
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert failing_node in body["failed_nodes"]
    assert healthy_node in body["nodes_queried"]
    assert body["total_results"] >= 1
    assert len(body["documents"]) >= 1
    assert body["documents"][0]["doc_id"] == "d7"
    # Partial responses must not be cached, so cached flag is False.
    assert body["cached"] is False


def test_cache_returns_same_response(test_env):
    tc = test_env["client"]
    ring = test_env["ring"]
    tokenizer = test_env["tokenizer"]

    content = "cacheworthy query phrase distinct tokens here"
    terms = tokenizer.tokenize(content)
    assert terms

    with respx.mock(assert_all_called=False) as mock:
        for url in NODE_URLS.values():
            mock.post(f"{url}/index_terms").mock(
                return_value=httpx.Response(200, json={"indexed": 1})
            )
        r = tc.post(
            "/index",
            json={"doc_id": "dc1", "content": content, "metadata": {}},
        )
        assert r.status_code == 200

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
                            "doc_ids": ["dc1"],
                            "doc_frequency": 1,
                        }
                    ],
                },
            )
        )

        payload = {"query": query_term, "op": "AND", "limit": 10}
        r1 = tc.post("/search", json=payload)
        assert r1.status_code == 200
        b1 = r1.json()
        assert b1["cached"] is False
        assert b1["total_results"] == 1

        r2 = tc.post("/search", json=payload)
        assert r2.status_code == 200
        b2 = r2.json()
        assert b2["cached"] is True
        assert b2["total_results"] == b1["total_results"]
        assert b2["documents"] == b1["documents"]


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
