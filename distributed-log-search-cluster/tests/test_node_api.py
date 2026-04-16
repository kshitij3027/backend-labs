"""Tests for the index node FastAPI app using TestClient + fakeredis."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from fakeredis import aioredis
from fastapi.testclient import TestClient

from node.main import create_app
from node.shard import NodeShard
from shared.config import NodeSettings


@pytest.fixture
def client():
    settings = NodeSettings(
        node_id="node-test", redis_host="localhost", redis_port=6379
    )
    app = create_app(settings)
    redis = aioredis.FakeRedis(decode_responses=True)

    @asynccontextmanager
    async def test_lifespan(a):
        a.state.shard = NodeShard("node-test", redis)
        a.state.settings = settings
        try:
            yield
        finally:
            await redis.aclose()

    app.router.lifespan_context = test_lifespan
    with TestClient(app) as c:
        yield c


def test_health_ok(client: TestClient):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert body["node_id"] == "node-test"


def test_stats_empty(client: TestClient):
    r = client.get("/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["node_id"] == "node-test"
    assert body["term_count"] == 0
    assert body["document_count"] == 0


def test_index_and_search_roundtrip(client: TestClient):
    r = client.post(
        "/index_terms",
        json={
            "doc_id": "d1",
            "terms": [
                {"term": "error", "tf": 3},
                {"term": "timeout", "tf": 1},
            ],
        },
    )
    assert r.status_code == 200
    assert r.json() == {"indexed": 2, "doc_id": "d1"}

    r = client.post(
        "/search_terms",
        json={"terms": ["error", "timeout", "missing"]},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["node_id"] == "node-test"
    by_term = {p["term"]: p for p in body["postings"]}
    assert by_term["error"]["doc_ids"] == ["d1"]
    assert by_term["error"]["doc_frequency"] == 1
    assert by_term["timeout"]["doc_ids"] == ["d1"]
    assert by_term["timeout"]["doc_frequency"] == 1
    assert by_term["missing"]["doc_ids"] == []
    assert by_term["missing"]["doc_frequency"] == 0


def test_stats_after_index(client: TestClient):
    client.post(
        "/index_terms",
        json={
            "doc_id": "d1",
            "terms": [
                {"term": "error", "tf": 2},
                {"term": "timeout", "tf": 1},
            ],
        },
    )
    client.post(
        "/index_terms",
        json={
            "doc_id": "d2",
            "terms": [
                {"term": "error", "tf": 1},
                {"term": "latency", "tf": 4},
            ],
        },
    )
    r = client.get("/stats")
    body = r.json()
    # 3 distinct terms: error, timeout, latency
    assert body["term_count"] == 3
    # 2 distinct docs
    assert body["document_count"] == 2
