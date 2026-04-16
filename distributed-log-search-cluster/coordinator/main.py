"""FastAPI application for the coordinator service."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import ConnectionPool, Redis

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

        app.state.registry = registry
        app.state.ring = ring
        app.state.tokenizer = tokenizer
        app.state.client = client
        app.state.redis = redis
        app.state.planner = QueryPlanner(
            registry, ring, tokenizer, client, redis
        )
        app.state.indexer = Indexer(registry, ring, tokenizer, client, redis)
        app.state.settings = settings
        try:
            yield
        finally:
            await close_client()
            await pool.disconnect()

    app = FastAPI(title="Coordinator", lifespan=lifespan)

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

    @app.post("/index")
    async def index(doc: DocumentInput) -> dict:
        return await app.state.indexer.index_document(doc)

    @app.post("/search", response_model=SearchResponse)
    async def search(req: SearchRequest) -> SearchResponse:
        return await app.state.planner.search(req)

    return app


app = create_app()
