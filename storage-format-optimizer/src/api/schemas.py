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


class StatsResponse(BaseModel):
    """Result of ``GET /api/stats`` — the system-wide dashboard snapshot.

    Assembled by :mod:`src.api.routes_stats` from the bounded
    :class:`~src.metrics.Metrics` snapshot **plus** manifest-derived layout: the
    metrics sub-documents (``storage`` / ``formats`` / ``performance`` /
    ``migrations`` / ``ingest``) are passed through largely as the aggregator
    returns them, with ``formats.distribution`` / ``formats.partitions_total``
    and ``storage.by_format`` filled in from the live manifest for accuracy. The
    sub-documents are typed as permissive ``dict`` fields because the assembler
    owns their exact shape; the dashboard reads well-known keys out of each.
    """

    storage: dict[str, Any] = Field(
        description=(
            "Storage totals: total_bytes, uncompressed_estimate_bytes, "
            "compression_ratio, and manifest-derived by_format byte sums."
        )
    )
    formats: dict[str, Any] = Field(
        description=(
            "Format layout: distribution (row/columnar/hybrid partition counts) "
            "and partitions_total, derived from the manifest."
        )
    )
    performance: dict[str, Any] = Field(
        description=(
            "Per-format query latency (p50/p90/throughput/count) and the "
            "analytical columnar-vs-row speedup."
        )
    )
    migrations: dict[str, Any] = Field(
        description="Migration activity: completed, failed, in_flight, recent."
    )
    ingest: dict[str, Any] = Field(
        description="Ingest activity: entries_per_sec and lifetime total_entries."
    )
    tenants: list[str] = Field(
        description="Sorted list of tenants that have a manifest on disk."
    )
    selection_optimality: float = Field(
        description=(
            "Fraction of partitions already in a format the selector would not "
            "change, in [0.0, 1.0]; 1.0 when there are no partitions."
        )
    )


class PartitionDecision(BaseModel):
    """One partition's current layout alongside the selector's recommendation.

    This is the per-partition row that powers the multi-tenant
    ``GET /api/stats/{tenant}`` view (Feature A): it shows what a partition *is*
    (current ``format`` / ``tier`` / counters / built index columns) next to what
    the :class:`~src.format_selector.FormatSelector` *recommends*, with the
    rule's human-readable ``reason`` and ``confidence`` so the dashboard can
    explain every decision.
    """

    partition_id: str = Field(description="Partition id (``p_<bucket>``).")
    format: str = Field(description="Current on-disk format value.")
    tier: str = Field(description="Current classified tier value (hot/warm/cold).")
    row_count: int = Field(description="Rows currently stored in the partition.")
    size_bytes: int = Field(description="On-disk size of the partition, in bytes.")
    recommended_format: str = Field(
        description="Format the selector recommends for this partition's access."
    )
    reason: str = Field(
        description="Human-readable explanation of the selector's recommendation."
    )
    confidence: float = Field(
        description="Selector confidence in the recommendation, in [0.0, 1.0]."
    )
    indexed_columns: list[str] = Field(
        description="Columns with a built min/max index on this partition."
    )


class TenantStatsResponse(BaseModel):
    """Result of ``GET /api/stats/{tenant}`` — one tenant's storage picture.

    Aggregates the tenant's partitions into format / tier distributions and
    rolled-up storage figures, and lists every partition's
    :class:`PartitionDecision`. A tenant with no partitions yields zeroed
    aggregates and empty collections (and a ``200``, never a ``404``).
    """

    tenant: str = Field(description="Tenant these stats describe.")
    format_distribution: dict[str, int] = Field(
        description="Partition counts keyed by format value (row/columnar/hybrid)."
    )
    tier_distribution: dict[str, int] = Field(
        description="Partition counts keyed by tier value (hot/warm/cold)."
    )
    partitions: list[PartitionDecision] = Field(
        description="Per-partition current layout + selector recommendation."
    )
    index_columns_total: int = Field(
        description="Total built index columns summed across the tenant's partitions."
    )
    storage_bytes: int = Field(
        description="Sum of on-disk size_bytes across the tenant's partitions."
    )
    compression_ratio: float = Field(
        description=(
            "Tenant compression ratio (sum uncompressed estimate / sum size); "
            "1.0 when nothing is stored."
        )
    )
