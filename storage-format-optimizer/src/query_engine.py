"""Read path — the query engine that fans a query across a tenant's partitions.

:class:`QueryEngine` is the single entry point FastAPI awaits for ``POST
/api/query``. Given a tenant and an optional projection / filters / aggregations
/ grouping, it:

1. **Classifies** the query's access shape via
   :func:`~src.classifier.classify_query` (analytical / full-record / mixed).
   The class is fed to the metrics aggregator and the pattern tracker so later
   commits can decide whether each partition should live in a row, columnar, or
   hybrid layout.
2. **Resolves a backend per partition.** Every partition in a tenant may be in a
   *different* on-disk format (a migration flips one at a time), so the engine
   looks up ``backends[meta.format]`` for each partition and lets that backend
   handle projection + predicate pushdown (columnar prunes row-groups; row/hybrid
   filter in Python — see :mod:`src.storage.base`).
3. **Aggregates in process.** Backends return raw (optionally projected, filtered)
   rows; ``count`` / ``sum`` / ``avg`` / ``min`` / ``max`` — with optional
   ``GROUP BY`` — are computed here over the unioned rows so the aggregation logic
   is format-agnostic and lives in one place.
4. **Records observations.** For every partition read it feeds the metrics
   aggregator (latency, scan accounting), the pattern tracker (read shape,
   columns touched, point-lookup-ness), and the index manager (per-filter
   selectivity), which together drive the adaptive format/index/tier decisions.

The engine owns **no** persistent state: the manifest is the source of truth for
partition layout, and the trackers/metrics are process-local observational
caches. Latencies are measured with :func:`time.perf_counter` (a monotonic, high
-resolution timer that is correct for *durations*); the ``now`` recorded for
access timestamps comes from the injected ``clock`` so behaviour is deterministic
under test.

The ``read`` body uses the **synchronous** backend API even though
:meth:`QueryEngine.query` is ``async`` — the per-partition reads are CPU/IO-bound
local file decodes that the FastAPI handler simply awaits; there is no
event-loop-blocking concern at the scale this optimizer targets.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from src.classifier import classify_query
from src.index_manager import IndexManager
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.models import Aggregation, Filter, Format, QueryClass
from src.paths import partition_paths
from src.pattern_tracker import PatternTracker
from src.settings import Settings
from src.storage.base import StorageBackend

__all__ = ["QueryEngine", "QueryResult"]


@dataclass
class QueryResult:
    """The outcome of one :meth:`QueryEngine.query` call.

    Exactly one of :attr:`rows` / :attr:`aggregates` carries the payload, the
    other is ``None``, depending on whether the query asked for aggregations:

    * **No aggregations** → :attr:`rows` is the (optionally ``limit``-truncated)
      list of result dicts and :attr:`aggregates` is ``None``.
    * **With aggregations** → :attr:`aggregates` holds the computed values (see
      :meth:`QueryEngine.query` for its exact shape, with and without
      ``group_by``) and :attr:`rows` is ``None``.

    :attr:`meta` is always present and reports per-query accounting:
    ``query_class``, ``partitions_read``, ``partitions_skipped`` (partitions the
    min/max index let us skip reading entirely), ``formats_used`` (format value →
    partitions read in that format), ``rows_scanned``, ``rowgroups_skipped`` and
    the total wall-clock ``elapsed_ms``.
    """

    rows: list[dict] | None
    aggregates: dict | None
    meta: dict


class QueryEngine:
    """Fans a query across a tenant's partitions and aggregates the result.

    Construct one per process, wired to the shared manifest store, the
    per-:class:`~src.models.Format` backend instances, and the observational
    sinks (pattern tracker, index manager, metrics). It is stateless beyond
    those injected collaborators, so a single instance safely serves concurrent
    queries.
    """

    def __init__(
        self,
        *,
        manifest: ManifestStore,
        backends: dict[Format, StorageBackend],
        pattern_tracker: PatternTracker,
        index_manager: IndexManager,
        metrics: Metrics,
        settings: Settings,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Wire the engine to its collaborators.

        Args:
            manifest: Source of truth for which partitions a tenant has and what
                format/index each is in.
            backends: Map from :class:`~src.models.Format` to the
                :class:`~src.storage.base.StorageBackend` that materialises it.
                Must contain an entry for every format a partition can be in.
            pattern_tracker: Records per-partition read shape (columns touched,
                point-lookup vs. scan, query class) for the format selector.
            index_manager: Records per-column filter selectivity to decide which
                columns are worth indexing.
            metrics: Aggregates per-format latency and scan accounting for the
                dashboard.
            settings: Runtime configuration (``data_dir`` and the query
                classification thresholds are read here).
            clock: Injectable wall-clock source (seconds) used only for the
                access timestamp passed to the pattern tracker. Latencies use
                :func:`time.perf_counter` regardless. Defaults to
                :func:`time.time`.
        """
        self._manifest = manifest
        self._backends = backends
        self._pattern_tracker = pattern_tracker
        self._index_manager = index_manager
        self._metrics = metrics
        self._settings = settings
        self._clock = clock

    async def query(
        self,
        tenant: str,
        *,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        aggregations: list[Aggregation] | None = None,
        group_by: list[str] | None = None,
        limit: int | None = None,
    ) -> QueryResult:
        """Run a query against ``tenant`` and return rows *or* aggregates.

        The query is classified, fanned across every partition the tenant owns
        (each read through its own format's backend), and the unioned rows are
        either returned directly (with an optional ``limit``) or reduced by the
        requested aggregations. Side effects — metrics, pattern, and index
        observations — are recorded per partition as a by-product.

        Args:
            tenant: Tenant whose data is queried.
            columns: Top-level keys to project; ``None`` reads the full record.
            filters: Predicates to AND together (pushed into each backend).
            aggregations: Aggregations to compute over the result rows; empty/
                ``None`` means "return rows, don't aggregate".
            group_by: Columns to group aggregations by; empty/``None`` means a
                single overall aggregate row.
            limit: When not aggregating, cap the returned rows to this many
                (applied after the union across partitions). Ignored for
                aggregations.

        Returns:
            A :class:`QueryResult` whose ``rows`` xor ``aggregates`` is populated
            per the rules above, plus a ``meta`` accounting dict.
        """
        t_start = time.perf_counter()
        now = self._clock()
        filters = filters or []
        aggregations = aggregations or []
        group_by = group_by or []

        qclass = classify_query(
            columns,
            aggregations,
            analytical_max=self._settings.analytical_max_columns,
            full_record_min=self._settings.full_record_min_columns,
        )

        # A read is a "point lookup" when it targets a single record: either a
        # full-record fetch pinned by an equality predicate, or an explicitly
        # tiny limit. This signal pushes a partition toward a row layout.
        is_point_lookup = (
            qclass == QueryClass.FULL_RECORD
            and any(f.op == "eq" for f in filters)
        ) or (limit is not None and limit <= 10)

        # Candidate partitions: read ALL of the tenant's partitions. Correct,
        # though not maximally optimized — a query with a ts bound could prune
        # partitions whose time-bucket window cannot overlap the bound by parsing
        # ``p_<bucket>`` -> ``[bucket*bucket_seconds, (bucket+1)*bucket_seconds)``.
        # TODO: ts pruning — skip partitions outside the filtered ts window.
        parts: list[PartitionMeta] = self._manifest.list_partitions(tenant)

        all_rows: list[dict] = []
        rows_scanned = 0
        rowgroups_skipped = 0
        formats_used: dict[str, int] = {}
        partitions_read = 0
        partitions_skipped = 0

        for meta in parts:
            paths = partition_paths(
                self._settings.data_dir, tenant, meta.partition_id
            )
            backend = self._backends[meta.format]

            # --- Index-driven partition skip (Feature C) ---
            # Before paying to decode this partition, consult its persisted
            # min/max index: if a filtered, indexed column proves the partition's
            # range cannot overlap the predicate, the partition holds no matching
            # row and we skip reading it entirely. ``partition_can_match`` is
            # sound — it only returns False on a *proven* miss — so this never
            # drops rows that should have been returned. Partitions without an
            # index (empty ``stats``) always fall through to a normal read.
            stats = (meta.index or {}).get("stats", {})
            if stats and filters:
                can_match, decisive = self._index_manager.partition_can_match(
                    stats, filters
                )
                if not can_match:
                    # Skipped work is the benefit the index just delivered: count
                    # the whole partition's rows as skipped, treat it as one
                    # skipped "row-group", and credit each decisive column so an
                    # index that keeps skipping work stays alive (and one that
                    # never skips trends toward being dropped).
                    rowgroups_skipped += 1
                    partitions_skipped += 1
                    for col in decisive:
                        self._index_manager.record_benefit(
                            tenant,
                            meta.partition_id,
                            col,
                            rows_skipped=meta.row_count,
                            rows_total=max(meta.row_count, 1),
                        )
                    continue

            p0 = time.perf_counter()
            rr = backend.read(
                paths,
                columns=columns,
                filters=filters,
                index=meta.index or None,
            )
            elapsed_ms = (time.perf_counter() - p0) * 1000

            all_rows += rr.rows
            rows_scanned += rr.rows_scanned
            rowgroups_skipped += rr.rowgroups_skipped
            formats_used[meta.format.value] = (
                formats_used.get(meta.format.value, 0) + 1
            )
            partitions_read += 1

            # Per-partition observations feeding the adaptive engines.
            self._metrics.record_query(
                meta.format.value,
                elapsed_ms,
                query_class=qclass.value,
                rows_scanned=rr.rows_scanned,
                rowgroups_skipped=rr.rowgroups_skipped,
            )
            self._pattern_tracker.record_read(
                tenant,
                meta.partition_id,
                columns=columns,
                query_class=qclass,
                is_point_lookup=is_point_lookup,
                ts=now,
            )
            # Selectivity = fraction of scanned rows this partition's read kept.
            # ``rows_scanned`` is pre-filter work; guard against divide-by-zero.
            selectivity = len(rr.rows) / max(rr.rows_scanned, 1)
            for f in filters:
                self._index_manager.note_filter(
                    tenant,
                    meta.partition_id,
                    f.column,
                    selectivity=selectivity,
                )

        # --- reduce: rows passthrough, or in-process aggregation ---
        if not aggregations:
            rows: list[dict] | None = (
                all_rows[:limit] if limit is not None else all_rows
            )
            aggregates: dict | None = None
        else:
            rows = None
            if not group_by:
                # Single overall aggregate: one flat dict of metric -> value.
                aggregates = self._aggregate(all_rows, aggregations)
            else:
                # Grouped aggregation: one dict per distinct group-key tuple,
                # carrying the group columns plus each computed metric.
                groups: list[dict] = []
                for key, bucket in self._group_rows(all_rows, group_by).items():
                    group_record: dict[str, Any] = dict(zip(group_by, key))
                    group_record.update(self._aggregate(bucket, aggregations))
                    groups.append(group_record)
                aggregates = {"group_by": list(group_by), "groups": groups}

        meta_out = {
            "query_class": qclass.value,
            "partitions_read": partitions_read,
            "partitions_skipped": partitions_skipped,
            "formats_used": formats_used,
            "rows_scanned": rows_scanned,
            "rowgroups_skipped": rowgroups_skipped,
            "elapsed_ms": (time.perf_counter() - t_start) * 1000,
        }
        return QueryResult(rows=rows, aggregates=aggregates, meta=meta_out)

    # ------------------------------------------------------------------ #
    # In-process aggregation helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _group_rows(
        rows: list[dict], group_by: list[str]
    ) -> dict[tuple, list[dict]]:
        """Bucket ``rows`` by the tuple of their ``group_by`` column values.

        A missing group column reads as ``None`` for that row (so all rows
        lacking the column fall into the same bucket). Insertion order of
        first-seen group keys is preserved (plain ``dict`` ordering), making the
        grouped output deterministic for a given input.
        """
        buckets: dict[tuple, list[dict]] = {}
        for row in rows:
            key = tuple(row.get(col) for col in group_by)
            buckets.setdefault(key, []).append(row)
        return buckets

    @staticmethod
    def _aggregate(
        rows: list[dict], aggregations: list[Aggregation]
    ) -> dict[str, Any]:
        """Compute the requested aggregations over ``rows``.

        Each aggregation produces one key ``f"{op}_{column or 'all'}"`` mapping to
        its value, so e.g. ``count`` with no column is ``"count_all"`` and
        ``sum`` of ``"latency_ms"`` is ``"sum_latency_ms"``. The output is a flat
        ``dict`` (used directly for an overall aggregate, or merged with the group
        columns for each group).

        Semantics (None values are treated as "absent"):

        * ``count`` with ``column is None`` → total row count.
        * ``count`` with a column → number of rows whose value is non-``None``.
        * ``sum`` → sum of the column's non-``None`` numeric values; ``0`` when
          there are none (an empty sum is zero).
        * ``avg`` → mean of the non-``None`` numeric values, or ``None`` when
          there are none (an undefined mean is reported as ``None``, not ``0``).
        * ``min`` / ``max`` → over the non-``None`` numeric values, or ``None``
          when there are none.
        """
        out: dict[str, Any] = {}
        for agg in aggregations:
            col = agg.column
            label = f"{agg.op}_{col or 'all'}"

            if agg.op == "count":
                if col is None:
                    out[label] = len(rows)
                else:
                    out[label] = sum(
                        1 for r in rows if r.get(col) is not None
                    )
                continue

            # sum / avg / min / max operate over the non-None values of ``col``.
            values = [
                r.get(col) for r in rows if col is not None and r.get(col) is not None
            ]
            if agg.op == "sum":
                out[label] = sum(values)  # empty sum -> 0
            elif agg.op == "avg":
                out[label] = (sum(values) / len(values)) if values else None
            elif agg.op == "min":
                out[label] = min(values) if values else None
            elif agg.op == "max":
                out[label] = max(values) if values else None
        return out
