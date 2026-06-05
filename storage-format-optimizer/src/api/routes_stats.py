"""Stats + per-tenant multi-tenant surface (Feature A).

Three read-only endpoints power the dashboard and expose the multi-tenant
storage picture:

* ``GET /api/stats`` — the system-wide snapshot. It folds the bounded
  :class:`~src.metrics.Metrics` aggregator's ``snapshot()`` together with
  **manifest-derived** layout (the manifest is the source of truth for which
  partitions exist and in what format) and a single scalar,
  ``selection_optimality``, summarising how much of the corpus is already laid
  out the way the :class:`~src.format_selector.FormatSelector` would choose.
* ``GET /api/stats/{tenant}`` — one tenant's partitions, each shown as a
  :class:`~src.api.schemas.PartitionDecision`: current format/tier next to the
  selector's recommended format + reason + confidence (Feature A's
  "explain every partition" view), plus rolled-up distributions and storage.
* ``GET /api/partitions?tenant=<t>`` — the raw per-partition manifest records
  for one tenant, for tooling / debugging.

The route is a *projection* layer: it owns no product state. It reads the
metrics aggregator, the manifest, and the live policy engines (selector / tier
manager / pattern tracker) and assembles JSON. Every path is robust to an empty
system — no tenants, no partitions — returning sane zeros rather than raising:
``/api/stats`` then reports ``selection_optimality=1.0`` and ``tenants=[]``.

The per-partition decision reuses the *exact* same inputs the migration engine
uses (live access stats, bucket-derived age, the tier manager's classification),
so the dashboard's "recommended format / reason" matches what the background
loop would actually do. A single ``now`` (one ``time.time()`` read per request)
is threaded through all age/tier math so the snapshot is internally consistent.
"""

from __future__ import annotations

import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import (
    get_manifest,
    get_metrics,
    get_pattern_tracker,
    get_selector,
    get_settings_dep,
    get_tier_manager,
)
from src.api.schemas import PartitionDecision, StatsResponse, TenantStatsResponse
from src.format_selector import FormatSelector
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.models import Format
from src.pattern_tracker import PatternTracker
from src.settings import Settings
from src.tier_manager import TierManager

router = APIRouter(prefix="/api", tags=["stats"])

# Canonical format keys, in dashboard display order (mirrors ``Format`` values).
_FORMATS: tuple[str, ...] = (
    Format.ROW.value,
    Format.COLUMNAR.value,
    Format.HYBRID.value,
)


def _partition_age_seconds(pid: str, bucket_seconds: int, now: float) -> float:
    """Return how long ago the partition's time-bucket began, in seconds.

    Mirrors :meth:`src.migration_engine.MigrationEngine._age_seconds`: the bucket
    index is parsed from the partition id (``pid[2:]`` strips the ``"p_"``
    prefix) and the bucket's start wall-clock time is ``bucket * bucket_seconds``.
    The result is floored at ``0.0`` so a clock-skewed future bucket never
    reports a negative age, and a malformed id (non-integer suffix) yields
    ``0.0`` rather than raising — one bad partition can never break the stats
    assembly.

    Args:
        pid: The partition id (expected shape ``p_<bucket>``).
        bucket_seconds: The time-bucket width (``settings.partition_bucket_seconds``).
        now: The current wall-clock time in seconds (shared across the request).

    Returns:
        ``max(0.0, now - bucket_start)`` seconds.
    """
    try:
        bucket = int(pid[2:])
    except (ValueError, IndexError):
        return 0.0
    start = bucket * bucket_seconds
    return max(0.0, now - start)


def _decide_partition(
    tenant: str,
    meta: PartitionMeta,
    *,
    selector: FormatSelector,
    tier_manager: TierManager,
    pattern_tracker: PatternTracker,
    bucket_seconds: int,
    now: float,
) -> tuple[PartitionDecision, bool]:
    """Build one partition's decision row and whether it is already optimal.

    Computes the same inputs the migration engine consults — the live access
    stats, the bucket-derived age, and the tier manager's classification — then
    asks the selector for a recommendation. The returned
    :class:`~src.api.schemas.PartitionDecision` pairs the partition's *current*
    layout with that recommendation (format + reason + confidence) and its built
    index columns.

    The boolean is the partition's "optimality": ``True`` when
    :meth:`~src.format_selector.FormatSelector.should_migrate` is ``False`` —
    i.e. the partition is already in a format the selector would not change
    (either it matches the recommendation, or the recommendation's confidence is
    below the migration floor). This is the per-partition term aggregated into
    the system-wide ``selection_optimality``.

    Args:
        tenant: Tenant identifier.
        meta: The partition's current manifest record.
        selector: The format selector (recommendation + migration gate).
        tier_manager: The tier classifier.
        pattern_tracker: Source of the partition's live access stats.
        bucket_seconds: Time-bucket width for the age computation.
        now: Shared current time for age/tier math.

    Returns:
        ``(decision, is_optimal)``.
    """
    stats = pattern_tracker.get_stats(tenant, meta.partition_id)
    age = _partition_age_seconds(meta.partition_id, bucket_seconds, now)
    tier = tier_manager.tier_for(stats, age_seconds=age, now=now)
    rec = selector.recommend(
        stats,
        age_seconds=age,
        row_count=meta.row_count,
        tier=tier,
        current_format=meta.format,
    )
    is_optimal = not selector.should_migrate(rec, meta.format)
    decision = PartitionDecision(
        partition_id=meta.partition_id,
        format=meta.format.value,
        tier=tier.value,
        row_count=meta.row_count,
        size_bytes=meta.size_bytes,
        recommended_format=rec.format.value,
        reason=rec.reason,
        confidence=rec.confidence,
        indexed_columns=list(meta.index.get("columns", [])),
    )
    return decision, is_optimal


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    metrics: Annotated[Metrics, Depends(get_metrics)],
    manifest: Annotated[ManifestStore, Depends(get_manifest)],
    selector: Annotated[FormatSelector, Depends(get_selector)],
    tier_manager: Annotated[TierManager, Depends(get_tier_manager)],
    pattern_tracker: Annotated[PatternTracker, Depends(get_pattern_tracker)],
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> StatsResponse:
    """Return the system-wide dashboard snapshot.

    Starts from the bounded metrics :meth:`~src.metrics.Metrics.snapshot` (query
    latency, ingest rate, migration counters, raw storage totals) and overlays
    **manifest-derived** layout for accuracy:

    * **Format distribution + per-format bytes** are recomputed by walking every
      tenant's partitions: each partition increments its format's count and adds
      its ``size_bytes`` to that format's byte total. These overwrite
      ``formats.distribution`` (plus a ``partitions_total``) and
      ``storage.by_format`` in the snapshot, and the storage ``total_bytes`` /
      ``uncompressed_estimate_bytes`` / ``compression_ratio`` are recomputed from
      the same manifest walk so the headline figures match the layout exactly.
    * **selection_optimality** is the fraction of partitions already in a format
      the selector would not change (see :func:`_decide_partition`), or ``1.0``
      when there are no partitions.

    Fully robust to an empty system: with no tenants the walk contributes
    nothing, ``compression_ratio`` falls back to ``1.0``, ``tenants`` is empty,
    and ``selection_optimality`` is ``1.0``.
    """
    now = time.time()
    snap = metrics.snapshot()

    # Single manifest walk: format distribution, per-format byte sums, totals,
    # and the optimality numerator/denominator — all derived from the live
    # source of truth rather than the observational metrics counters.
    distribution: dict[str, int] = {f: 0 for f in _FORMATS}
    by_format_bytes: dict[str, int] = {f: 0 for f in _FORMATS}
    total_bytes = 0
    uncompressed_estimate = 0
    partitions_total = 0
    optimal_count = 0

    tenants = manifest.all_tenants()
    for tenant in tenants:
        for meta in manifest.list_partitions(tenant):
            partitions_total += 1
            fmt = meta.format.value
            # Unknown formats (shouldn't happen) get their own bucket so nothing
            # is silently dropped from the totals.
            distribution[fmt] = distribution.get(fmt, 0) + 1
            by_format_bytes[fmt] = by_format_bytes.get(fmt, 0) + int(meta.size_bytes)
            total_bytes += int(meta.size_bytes)
            uncompressed_estimate += int(meta.uncompressed_estimate_bytes)

            _decision, is_optimal = _decide_partition(
                tenant,
                meta,
                selector=selector,
                tier_manager=tier_manager,
                pattern_tracker=pattern_tracker,
                bucket_seconds=settings.partition_bucket_seconds,
                now=now,
            )
            if is_optimal:
                optimal_count += 1

    # Overlay manifest-derived layout onto the metrics snapshot.
    snap["formats"]["distribution"] = distribution
    snap["formats"]["partitions_total"] = partitions_total
    snap["storage"]["by_format"] = by_format_bytes
    snap["storage"]["total_bytes"] = total_bytes
    snap["storage"]["uncompressed_estimate_bytes"] = uncompressed_estimate
    snap["storage"]["compression_ratio"] = (
        uncompressed_estimate / total_bytes if total_bytes > 0 else 1.0
    )

    # Optimality is 1.0 for an empty system (nothing is mis-formatted).
    selection_optimality = (
        optimal_count / partitions_total if partitions_total else 1.0
    )

    return StatsResponse(
        storage=snap["storage"],
        formats=snap["formats"],
        performance=snap["performance"],
        migrations=snap["migrations"],
        ingest=snap["ingest"],
        tenants=sorted(tenants),
        selection_optimality=selection_optimality,
    )


@router.get("/stats/{tenant}", response_model=TenantStatsResponse)
async def get_tenant_stats(
    tenant: str,
    manifest: Annotated[ManifestStore, Depends(get_manifest)],
    selector: Annotated[FormatSelector, Depends(get_selector)],
    tier_manager: Annotated[TierManager, Depends(get_tier_manager)],
    pattern_tracker: Annotated[PatternTracker, Depends(get_pattern_tracker)],
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> TenantStatsResponse:
    """Return one tenant's storage picture with per-partition decisions.

    For every partition the tenant owns, builds a
    :class:`~src.api.schemas.PartitionDecision` (current format/tier next to the
    selector's recommendation + reason + confidence, and the built index
    columns), and rolls the partitions up into:

    * ``format_distribution`` / ``tier_distribution`` — partition counts keyed by
      the partition's *current* format and its freshly classified tier;
    * ``index_columns_total`` — built index columns summed across partitions;
    * ``storage_bytes`` — summed on-disk size;
    * ``compression_ratio`` — ``sum(uncompressed_estimate) / sum(size_bytes)``,
      falling back to ``1.0`` when the tenant stores nothing.

    An unknown tenant (or one with no partitions) yields zeroed aggregates and
    empty collections with a ``200`` — never a ``404`` — so the dashboard can
    render a brand-new tenant cleanly.
    """
    now = time.time()

    decisions: list[PartitionDecision] = []
    format_distribution: dict[str, int] = {}
    tier_distribution: dict[str, int] = {}
    index_columns_total = 0
    storage_bytes = 0
    uncompressed_estimate = 0

    for meta in manifest.list_partitions(tenant):
        decision, _is_optimal = _decide_partition(
            tenant,
            meta,
            selector=selector,
            tier_manager=tier_manager,
            pattern_tracker=pattern_tracker,
            bucket_seconds=settings.partition_bucket_seconds,
            now=now,
        )
        decisions.append(decision)

        format_distribution[decision.format] = (
            format_distribution.get(decision.format, 0) + 1
        )
        tier_distribution[decision.tier] = (
            tier_distribution.get(decision.tier, 0) + 1
        )
        index_columns_total += len(decision.indexed_columns)
        storage_bytes += int(meta.size_bytes)
        uncompressed_estimate += int(meta.uncompressed_estimate_bytes)

    compression_ratio = (
        uncompressed_estimate / storage_bytes if storage_bytes > 0 else 1.0
    )

    return TenantStatsResponse(
        tenant=tenant,
        format_distribution=format_distribution,
        tier_distribution=tier_distribution,
        partitions=decisions,
        index_columns_total=index_columns_total,
        storage_bytes=storage_bytes,
        compression_ratio=compression_ratio,
    )


@router.get("/partitions")
async def list_partitions(
    manifest: Annotated[ManifestStore, Depends(get_manifest)],
    tenant: Annotated[str, Query(description="Tenant whose partitions to list.")] = (
        "default"
    ),
) -> list[dict[str, Any]]:
    """List the raw manifest records for one tenant's partitions.

    A thin projection of :meth:`~src.manifest.ManifestStore.list_partitions`:
    each partition is serialised via :meth:`~src.manifest.PartitionMeta.to_dict`
    (the full on-disk record — format, tier, paths, counters, codecs, index,
    access, last_migration). The ``tenant`` query parameter defaults to
    ``"default"`` so the endpoint is usable without arguments; an unknown tenant
    returns an empty list (``200``), never a ``404``.

    Args:
        manifest: The per-tenant manifest store.
        tenant: Tenant whose partitions to return (defaults to ``"default"``).

    Returns:
        A list of per-partition dicts (possibly empty).
    """
    return [meta.to_dict() for meta in manifest.list_partitions(tenant)]
