"""Pydantic response models for the optimizer's REST API.

These are the *output* shapes the ingest and query routes serialise. They mirror
the dicts returned by the engines (``IngestEngine.ingest`` -> a summary dict;
``QueryEngine.query`` -> a :class:`~src.query_engine.QueryResult`) so a route can
construct them with simple ``**`` unpacking, while still giving FastAPI a typed,
self-documenting schema for the OpenAPI docs.

Request bodies are *not* defined here — they live in :mod:`src.models`
(``IngestRequest`` / ``QueryRequest``) and drive FastAPI's automatic ``422``
validation of malformed payloads.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IngestResponse(BaseModel):
    """Result of a ``POST /api/ingest`` call.

    Built directly from :meth:`~src.ingest_engine.IngestEngine.ingest`'s summary
    dict (``{"ingested": ..., "partitions_touched": ..., "tenant": ...}``).
    """

    ingested: int = Field(
        description="Total number of rows landed across all touched partitions."
    )
    partitions_touched: list[str] = Field(
        description="Sorted partition ids that received at least one row."
    )
    tenant: str = Field(description="Tenant the batch was ingested for.")


class QueryMeta(BaseModel):
    """Per-query accounting returned alongside a query result.

    Mirrors the ``meta`` dict on :class:`~src.query_engine.QueryResult`: the
    classified access shape, how many partitions were read vs. skipped (via the
    min/max index), the per-format read breakdown, scan accounting, and the total
    wall-clock time.
    """

    query_class: str = Field(
        description="Classified access shape: 'analytical', 'full_record', or 'mixed'."
    )
    partitions_read: int = Field(
        description="Number of partitions actually decoded for this query."
    )
    partitions_skipped: int = Field(
        default=0,
        description="Partitions skipped entirely via the min/max index.",
    )
    formats_used: dict[str, int] = Field(
        description="Map of storage format value -> partitions read in that format."
    )
    rows_scanned: int = Field(
        description="Total rows scanned (pre-filter work) across read partitions."
    )
    rowgroups_skipped: int = Field(
        description="Row groups / partitions skipped via min/max statistics."
    )
    elapsed_ms: float = Field(
        description="Total wall-clock time for the query, in milliseconds."
    )


class QueryResponse(BaseModel):
    """Result of a ``POST /api/query`` call.

    Exactly one of :attr:`rows` / :attr:`aggregates` carries the payload
    (the other is ``None``), matching :class:`~src.query_engine.QueryResult`:
    a non-aggregating query returns ``rows``; an aggregating query returns
    ``aggregates``. :attr:`meta` is always present.
    """

    rows: list[dict] | None = Field(
        default=None,
        description="Result rows when the query did not aggregate; otherwise null.",
    )
    aggregates: dict[str, Any] | None = Field(
        default=None,
        description="Computed aggregates when the query aggregated; otherwise null.",
    )
    meta: QueryMeta = Field(description="Per-query accounting metadata.")
