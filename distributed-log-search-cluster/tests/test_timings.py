"""Unit test for per-stage search timing fields.

Verifies that a happy-path ``/search`` response exposes ``routing_ms``,
``scatter_ms`` and ``merge_ms`` as non-negative floats, and that their
sum does not exceed ``search_time_ms`` by more than a small slack
(timer overhead/scheduling jitter).
"""

from __future__ import annotations

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
        yield {"client": tc, "redis": redis, "ring": ring, "tokenizer": tokenizer}


def test_search_response_has_monotonic_stage_timings(test_env):
    tc = test_env["client"]
    ring = test_env["ring"]
    tokenizer = test_env["tokenizer"]

    content = "critical database error timeout service"
    terms = tokenizer.tokenize(content)
    assert terms

    with respx.mock(assert_all_called=False) as mock:
        for url in NODE_URLS.values():
            mock.post(f"{url}/index_terms").mock(
                return_value=httpx.Response(200, json={"indexed": 1})
            )
        r = tc.post("/index", json={"doc_id": "dt1", "content": content})
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
                            "doc_ids": ["dt1"],
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

    # Fields present and non-negative.
    for field in ("routing_ms", "scatter_ms", "merge_ms", "search_time_ms"):
        assert field in body, f"missing {field}"
        assert isinstance(body[field], (int, float))
        assert body[field] >= 0.0

    # Sum of stages must not exceed overall search_time_ms by more than
    # a small slack to cover timer/scheduling overhead.
    stage_sum = body["routing_ms"] + body["scatter_ms"] + body["merge_ms"]
    assert stage_sum - body["search_time_ms"] <= 5.0, (
        f"stage sum {stage_sum:.3f}ms exceeds search_time_ms "
        f"{body['search_time_ms']:.3f}ms by more than 5ms slack"
    )
