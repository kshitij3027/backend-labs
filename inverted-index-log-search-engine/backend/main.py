"""FastAPI application for the inverted index log search engine."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query

from backend.config import settings
from backend.models import (
    BulkIndexRequest,
    BulkIndexResponse,
    DocumentInput,
    HealthResponse,
    IndexResponse,
    SearchResponse,
    StatsResponse,
)
from backend.tokenizer import LogTokenizer
from backend.index import InvertedIndex
from backend.search import SearchEngine
from backend.sample_data import generate_sample_logs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup: create core components and seed with sample data
    tokenizer = LogTokenizer()
    index = InvertedIndex(tokenizer)
    search_engine = SearchEngine(index, tokenizer)

    app.state.tokenizer = tokenizer
    app.state.index = index
    app.state.search_engine = search_engine

    sample_logs = generate_sample_logs(10)
    await index.add_documents_bulk(sample_logs)

    yield
    # Shutdown: nothing to clean up yet


app = FastAPI(
    title="Inverted Index Log Search Engine",
    description="High-performance full-text search for log entries",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Return service health status and basic index stats."""
    stats = app.state.index.get_stats()
    return HealthResponse(
        status="healthy",
        documents=stats["total_documents"],
        terms=stats["total_terms"],
    )


@app.get("/api/search", response_model=SearchResponse)
async def search(
    q: str = Query("", description="Search query"),
    limit: int = Query(settings.SEARCH_RESULT_LIMIT, ge=1, le=1000),
) -> SearchResponse:
    """Full-text search across indexed log entries."""
    return app.state.search_engine.search(q, limit=limit)


@app.get("/api/stats", response_model=StatsResponse)
async def get_stats() -> StatsResponse:
    """Return aggregate index statistics."""
    stats = app.state.index.get_stats()
    return StatsResponse(**stats)


@app.post("/api/index", response_model=IndexResponse)
async def index_document(doc: DocumentInput) -> IndexResponse:
    """Index a single log document."""
    doc_id = await app.state.index.add_document(
        message=doc.message,
        timestamp=doc.timestamp,
        service=doc.service,
        level=doc.level,
    )
    return IndexResponse(doc_id=doc_id, message="Document indexed successfully")


@app.post("/api/index/bulk", response_model=BulkIndexResponse)
async def bulk_index(request: BulkIndexRequest) -> BulkIndexResponse:
    """Index multiple log documents in a single request."""
    doc_ids = await app.state.index.add_documents_bulk(request.documents)
    return BulkIndexResponse(doc_ids=doc_ids, count=len(doc_ids))


@app.get("/api/suggestions")
async def get_suggestions(
    prefix: str = Query("", description="Term prefix"),
    limit: int = Query(10, ge=1, le=50),
) -> dict:
    """Return autocomplete suggestions for a term prefix."""
    suggestions = app.state.search_engine.get_suggestions(prefix, limit=limit)
    return {"suggestions": suggestions}
