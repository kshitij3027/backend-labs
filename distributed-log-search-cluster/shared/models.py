"""Pydantic v2 models shared between coordinator and index nodes."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class DocumentInput(BaseModel):
    """A document to be indexed."""

    doc_id: str
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class TermTF(BaseModel):
    """A single term and its frequency within a document."""

    term: str
    tf: int


class IndexTermsRequest(BaseModel):
    """Request body for a node's ``/index_terms`` endpoint."""

    doc_id: str
    terms: list[TermTF]


class SearchTermsRequest(BaseModel):
    """Request body for a node's ``/search_terms`` endpoint."""

    terms: list[str]


class PostingEntry(BaseModel):
    """A posting list for a single term on a specific node."""

    term: str
    doc_ids: list[str]
    doc_frequency: int


class SearchTermsResponse(BaseModel):
    """Response from a node's ``/search_terms`` endpoint."""

    node_id: str
    postings: list[PostingEntry]


class SearchRequest(BaseModel):
    """Coordinator search request."""

    query: str
    op: Literal["AND", "OR"] = "AND"
    limit: int = 50


class SearchResultItem(BaseModel):
    """One result document in a search response."""

    doc_id: str
    content: str
    score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    """Coordinator search response."""

    documents: list[SearchResultItem]
    total_results: int
    search_time_ms: float
    nodes_queried: list[str]
    failed_nodes: list[str] = Field(default_factory=list)
    routing_ms: float = 0.0
    scatter_ms: float = 0.0
    merge_ms: float = 0.0


class HealthResponse(BaseModel):
    """Simple health response for a node or the coordinator."""

    status: str
    node_id: str | None = None


class NodeStatsResponse(BaseModel):
    """Per-node stats response."""

    node_id: str
    term_count: int
    document_count: int
    status: str = "active"


class ClusterHealthResponse(BaseModel):
    """Coordinator's cluster-wide health summary."""

    status: str
    coordinator_port: int
    nodes: dict[str, bool]
    healthy_nodes: int
    total_nodes: int
