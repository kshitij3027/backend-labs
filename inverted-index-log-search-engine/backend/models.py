"""Pydantic models for the inverted index log search engine."""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    documents: int
    terms: int


class DocumentInput(BaseModel):
    """Input model for indexing a single log document."""

    message: str
    timestamp: float
    service: str
    level: str


class DocumentMeta(BaseModel):
    """Metadata stored for each indexed document."""

    doc_id: int
    message: str
    timestamp: float
    service: str
    level: str
    term_count: int = 0


class SearchResult(BaseModel):
    """A single search result with relevance scoring."""

    doc_id: int
    message: str
    highlighted_message: str
    timestamp: float
    service: str
    level: str
    score: float


class SearchResponse(BaseModel):
    """Response wrapper for search queries."""

    results: list[SearchResult]
    total_results: int
    search_time_ms: float
    query: str


class StatsResponse(BaseModel):
    """Index statistics response."""

    total_documents: int
    total_terms: int
    avg_terms_per_doc: float


class IndexResponse(BaseModel):
    """Response after indexing a single document."""

    doc_id: int
    message: str


class BulkIndexRequest(BaseModel):
    """Request body for bulk indexing multiple documents."""

    documents: list[DocumentInput]


class BulkIndexResponse(BaseModel):
    """Response after bulk indexing."""

    doc_ids: list[int]
    count: int
