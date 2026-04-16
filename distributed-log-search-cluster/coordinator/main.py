"""FastAPI application for the coordinator service."""

from __future__ import annotations

import asyncio
import json as _json
import logging
import time as _time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import ConnectionPool, Redis

logging.basicConfig(level=logging.INFO, format="%(message)s")

from coordinator.cache import ResultCache
from coordinator.cluster import ClusterRegistry
from coordinator.http_client import close_client, get_client
from coordinator.indexer import Indexer
from coordinator.planner import QueryPlanner
from shared.config import CoordinatorSettings
from shared.hash_ring import ConsistentHashRing
from shared.models import (
    ClusterHealthResponse,
    DocumentInput,
    SearchRequest,
    SearchResponse,
)
from shared.tokenizer import LogTokenizer


def create_app(settings: CoordinatorSettings | None = None) -> FastAPI:
    settings = settings or CoordinatorSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        node_urls = settings.parsed_node_urls()
        registry = ClusterRegistry(node_urls)
        ring = ConsistentHashRing(virtual_nodes=settings.virtual_nodes)
        for nid in registry.nodes():
            ring.add_node(nid)
        tokenizer = LogTokenizer()
        client = get_client(timeout=settings.request_timeout)
        pool = ConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            db=0,
            decode_responses=True,
            max_connections=50,
        )
        redis = Redis(connection_pool=pool)

        cache = ResultCache(size=settings.cache_size, ttl=settings.cache_ttl)

        app.state.registry = registry
        app.state.ring = ring
        app.state.tokenizer = tokenizer
        app.state.client = client
        app.state.redis = redis
        app.state.cache = cache
        app.state.planner = QueryPlanner(
            registry,
            ring,
            tokenizer,
            client,
            redis,
            cache=cache,
            retry_count=settings.retry_count,
            retry_base_delay=settings.retry_base_delay,
        )
        app.state.indexer = Indexer(registry, ring, tokenizer, client, redis)
        app.state.settings = settings
        try:
            yield
        finally:
            await close_client()
            await pool.disconnect()

    app = FastAPI(title="Coordinator", lifespan=lifespan)

    @app.middleware("http")
    async def log_requests(request, call_next):
        t0 = _time.perf_counter()
        response = await call_next(request)
        dt_ms = (_time.perf_counter() - t0) * 1000
        logging.getLogger("coordinator").info(
            _json.dumps(
                {
                    "path": request.url.path,
                    "method": request.method,
                    "status": response.status_code,
                    "duration_ms": round(dt_ms, 2),
                }
            )
        )
        return response

    @app.get("/health", response_model=ClusterHealthResponse)
    async def health() -> ClusterHealthResponse:
        statuses = await app.state.registry.check_health(app.state.client)
        healthy = sum(1 for v in statuses.values() if v)
        return ClusterHealthResponse(
            status="healthy" if healthy > 0 else "unhealthy",
            coordinator_port=app.state.settings.coordinator_port,
            nodes=statuses,
            healthy_nodes=healthy,
            total_nodes=len(statuses),
        )

    @app.get("/cluster")
    async def cluster() -> dict:
        return {
            "nodes": app.state.registry.nodes(),
            "virtual_nodes": app.state.settings.virtual_nodes,
        }

    @app.get("/cluster/stats")
    async def cluster_stats() -> dict:
        client = app.state.client
        registry = app.state.registry
        nodes = registry.nodes()

        async def fetch(nid: str):
            base = registry.url_for(nid)
            if base is None:
                return nid, None
            url = base.rstrip("/") + "/stats"
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    return nid, r.json()
            except Exception:
                pass
            return nid, None

        results = await asyncio.gather(*(fetch(n) for n in nodes))
        return {nid: stats for nid, stats in results}

    @app.post("/index")
    async def index(doc: DocumentInput) -> dict:
        return await app.state.indexer.index_document(doc)

    @app.post("/search", response_model=SearchResponse)
    async def search(req: SearchRequest) -> SearchResponse:
        return await app.state.planner.search(req)

    return app


app = create_app()
