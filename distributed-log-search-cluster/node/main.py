"""FastAPI application for a single index node."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from node.redis_client import close_redis, get_redis
from node.shard import NodeShard
from shared.config import NodeSettings
from shared.models import (
    HealthResponse,
    IndexTermsRequest,
    NodeStatsResponse,
    PostingEntry,
    SearchTermsRequest,
    SearchTermsResponse,
)


def create_app(settings: NodeSettings | None = None) -> FastAPI:
    """Build an index-node FastAPI app. Settings can be injected for tests."""
    settings = settings or NodeSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        redis = get_redis(settings.redis_host, settings.redis_port, settings.redis_db)
        app.state.shard = NodeShard(settings.node_id, redis)
        app.state.settings = settings
        try:
            yield
        finally:
            await close_redis()

    app = FastAPI(title=f"IndexNode-{settings.node_id}", lifespan=lifespan)

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        try:
            ok = await app.state.shard.ping()
            return HealthResponse(
                status="healthy" if ok else "unhealthy",
                node_id=settings.node_id,
            )
        except Exception:
            return HealthResponse(status="unhealthy", node_id=settings.node_id)

    @app.get("/stats", response_model=NodeStatsResponse)
    async def stats() -> NodeStatsResponse:
        s = await app.state.shard.stats()
        return NodeStatsResponse(
            node_id=settings.node_id,
            term_count=s["term_count"],
            document_count=s["document_count"],
        )

    @app.post("/index_terms")
    async def index_terms(req: IndexTermsRequest) -> dict:
        count = await app.state.shard.index_terms(req.doc_id, req.terms)
        return {"indexed": count, "doc_id": req.doc_id}

    @app.post("/search_terms", response_model=SearchTermsResponse)
    async def search_terms(req: SearchTermsRequest) -> SearchTermsResponse:
        results = await app.state.shard.get_postings_batch(req.terms)
        postings = [
            PostingEntry(term=t, doc_ids=dids, doc_frequency=df)
            for t, dids, df in results
        ]
        return SearchTermsResponse(node_id=settings.node_id, postings=postings)

    return app


app = create_app()
