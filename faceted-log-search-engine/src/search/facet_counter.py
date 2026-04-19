"""Execute the query-builder SQL and shape the HTTP response.

``query_builder`` produces raw ``(sql, params)`` tuples; this module
runs them against an ``aiosqlite.Connection``, slices the results
correctly for keyset pagination, post-processes the UNION ALL facet
rows into a list of ``FacetSummary`` objects, and stamps a
``query_time_ms`` measured across both queries.

Responsibilities:

* Run the results SQL and carve off the ``has_more`` / ``next_cursor``.
* Run the facet SQL and group its rows by dimension.
* Ensure every facet dimension is present in the response (even when
  a dimension has zero counts after filtering) so the UI always sees
  a stable shape.
* Keep selected-but-zero-count values visible (with ``selected=True``
  and ``count=0``) so the user never loses their checkbox.
* Sort each dimension's values by count desc → value asc and
  truncate to ``settings.max_facet_values``.

The ``search`` coroutine is what the HTTP layer calls; the ``facets``
helper is the facets-only path used by ``GET /api/facets``.

Both entry points accept *either* an ``AsyncSqlitePool`` (the HTTP
layer's production path, for read-connection fan-out) or a raw
``aiosqlite.Connection`` (what unit tests pass directly). We detect
the type at the entry and dispatch to the shared ``_search_impl`` /
``_facets_only_impl`` so neither caller has to adapt.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Mapping, Optional, Union

import aiosqlite

from src.config import settings
from src.models import (
    FacetSummary,
    FacetValue,
    FacetsOnlyResponse,
    SearchResponse,
    FACET_DISPLAY_NAMES,
)
from src.search import query_builder
from src.search.query_builder import FACET_DIMS, RESULT_COLUMNS
from src.storage.sqlite_store import AsyncSqlitePool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Row shaping helpers
# ---------------------------------------------------------------------------

def _row_to_log_dict(row: aiosqlite.Row) -> Dict[str, Any]:
    """Convert a result row into the dict we return to clients.

    ``metadata`` lives as JSON-encoded TEXT in SQLite. We parse it back
    to a dict here so the HTTP layer doesn't have to. Malformed JSON
    is logged and treated as an empty dict — don't fail the whole
    request over one bad row.
    """
    out: Dict[str, Any] = {col: row[col] for col in RESULT_COLUMNS if col != "metadata"}
    raw_meta = row["metadata"]
    if raw_meta is None or raw_meta == "":
        out["metadata"] = {}
    else:
        try:
            out["metadata"] = json.loads(raw_meta)
        except json.JSONDecodeError:
            logger.warning("row %s has non-JSON metadata; treating as {}", row["id"])
            out["metadata"] = {}
    return out


def _normalize_filters(
    filters: Optional[Mapping[str, List[Any]]],
) -> Dict[str, List[Any]]:
    """Return a defensive copy with only recognized facet dimensions.

    Unknown keys are dropped silently (per spec). Empty value lists
    are also dropped so downstream callers don't have to guard.
    """
    out: Dict[str, List[Any]] = {}
    if not filters:
        return out
    for dim, vals in filters.items():
        if dim not in FACET_DIMS:
            continue
        if not vals:
            continue
        out[dim] = list(vals)
    return out


# ---------------------------------------------------------------------------
# Facet post-processing
# ---------------------------------------------------------------------------

def _build_facet_summaries(
    raw_rows: List[aiosqlite.Row],
    filters: Mapping[str, List[Any]],
    max_values: int,
) -> List[FacetSummary]:
    """Group the UNION ALL facet rows into one ``FacetSummary`` per dim.

    Steps:
      1. Bucket rows by ``facet`` column (the dim name literal).
      2. Sort each bucket by count desc, value asc.
      3. Mark values that overlap with ``filters`` as ``selected``.
      4. Truncate to ``max_values`` and set ``has_more_values``.
      5. Re-attach selected-but-zero values that were filtered out by
         other dimensions so the UI checkbox stays checked.
    """
    # Group rows: dim -> list[(value, count)]
    grouped: Dict[str, List[tuple[Any, int]]] = {dim: [] for dim in FACET_DIMS}
    for row in raw_rows:
        dim = row["facet"]
        val = row["value"]
        count = int(row["c"])
        if dim not in grouped:
            continue
        grouped[dim].append((val, count))

    summaries: List[FacetSummary] = []
    for dim in FACET_DIMS:
        selected_vals = set(filters.get(dim, []) or [])
        # Normalize selected_vals to match the type returned by SQLite:
        # hour_bucket comes back as int, everything else as str.
        if dim == "hour_bucket":
            norm_selected = {int(v) for v in selected_vals}
        else:
            norm_selected = {str(v) for v in selected_vals}

        rows = grouped.get(dim, [])
        # Sort: count desc, then value asc for stable tie-breaking.
        rows.sort(key=lambda vc: (-vc[1], str(vc[0])))

        shown: List[FacetValue] = []
        seen_values: set = set()
        for val, cnt in rows:
            shown.append(
                FacetValue(
                    value=val,
                    count=cnt,
                    selected=val in norm_selected,
                )
            )
            seen_values.add(val)

        # Truncate to max_values. Keep track of whether truncation
        # happened before we re-inject zero-count selected entries so
        # "has_more_values" reflects the real data, not the cosmetic
        # tail we add back.
        has_more_values = len(shown) > max_values
        if has_more_values:
            shown = shown[:max_values]

        # Any currently-selected value that didn't come back from the
        # UNION (because OTHER filters wiped its count) must still be
        # visible — append at the end with count=0 so the UI doesn't
        # rip the checkbox away. We also append selected values that
        # fell off due to truncation so the user sees their pick.
        shown_values: set = {fv.value for fv in shown}
        for sel in norm_selected:
            if sel in shown_values:
                continue
            shown.append(
                FacetValue(value=sel, count=0, selected=True)
            )

        summaries.append(
            FacetSummary(
                name=dim,
                display_name=FACET_DISPLAY_NAMES.get(dim, dim),
                values=shown,
                has_more_values=has_more_values,
            )
        )

    return summaries


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

# Either an opaque read-pool (has ``.read()``) or a raw aiosqlite
# Connection. Production callers pass the pool so reads fan out across
# ``read_size`` handles; unit tests pass a raw conn for simplicity.
DbLike = Union[AsyncSqlitePool, aiosqlite.Connection]


async def _search_impl(
    conn: aiosqlite.Connection,
    norm_filters: Dict[str, List[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    cursor: Optional[int],
    limit: int,
    t0: int,
) -> SearchResponse:
    """Run the results + facet SQL sequentially on one connection.

    Used for the raw-conn (unit-test) path. Production goes through
    ``_search_impl_pool`` instead, which runs the two queries on two
    separate readers concurrently.
    """
    rows = await _run_results(
        conn, norm_filters, query, ts_start, ts_end, cursor, limit
    )
    facet_rows = await _run_facets(
        conn, norm_filters, query, ts_start, ts_end
    )

    has_more = len(rows) > limit
    sliced = rows[:limit] if has_more else rows
    logs_out = [_row_to_log_dict(r) for r in sliced]
    next_cursor = int(sliced[-1]["ts"]) if sliced and has_more else None
    total_count = None if has_more else len(sliced)

    facet_summaries = _build_facet_summaries(
        raw_rows=facet_rows,
        filters=norm_filters,
        max_values=settings.max_facet_values,
    )

    query_time_ms = (time.perf_counter_ns() - t0) / 1_000_000
    return SearchResponse(
        logs=logs_out,
        total_count=total_count,
        has_more=has_more,
        next_cursor=next_cursor,
        facets=facet_summaries,
        query_time_ms=round(query_time_ms, 3),
        applied_filters=norm_filters,
    )


async def _facets_only_impl(
    conn: aiosqlite.Connection,
    norm_filters: Dict[str, List[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    t0: int,
) -> FacetsOnlyResponse:
    """Run just the facet UNION ALL and shape a ``FacetsOnlyResponse``."""
    facet_rows = await _run_facets(
        conn, norm_filters, query, ts_start, ts_end
    )

    facet_summaries = _build_facet_summaries(
        raw_rows=facet_rows,
        filters=norm_filters,
        max_values=settings.max_facet_values,
    )

    query_time_ms = (time.perf_counter_ns() - t0) / 1_000_000
    return FacetsOnlyResponse(
        facets=facet_summaries,
        query_time_ms=round(query_time_ms, 3),
        applied_filters=norm_filters,
    )


async def _run_results(
    conn: aiosqlite.Connection,
    norm_filters: Dict[str, List[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    cursor: Optional[int],
    limit: int,
) -> List[aiosqlite.Row]:
    """Execute the paginated results SELECT and return the raw rows."""
    sql, params = query_builder.build_results_sql(
        filters=norm_filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
        cursor=cursor,
        limit=limit,
    )
    async with conn.execute(sql, params) as cur:
        return await cur.fetchall()


async def _run_facets(
    conn: aiosqlite.Connection,
    norm_filters: Dict[str, List[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
) -> List[aiosqlite.Row]:
    """Execute the facet UNION ALL and return the raw rows."""
    sql, params = query_builder.build_facet_sql(
        filters=norm_filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
    )
    async with conn.execute(sql, params) as cur:
        return await cur.fetchall()


async def search(
    conn: DbLike,
    filters: Optional[Mapping[str, List[Any]]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    cursor: Optional[int],
    limit: int,
) -> SearchResponse:
    """Run the results SQL + facet SQL and assemble a ``SearchResponse``.

    Accepts either an ``AsyncSqlitePool`` (production path, checks out
    a read connection for the duration of the call) or a raw
    ``aiosqlite.Connection`` (test convenience). Timing covers both
    queries plus any pool-checkout wait so the reported
    ``query_time_ms`` reflects real user-perceived latency.

    The two queries run sequentially on the SAME checked-out reader:
    aiosqlite's thread-per-connection model means parallel queries on
    different connections actually slow each other down (SQLite's
    internal mutex + GIL mean 2 parallel queries take 3x as long as
    one sequential pair). Running them back-to-back on one connection
    gives the best wall-clock per request.
    """
    t0 = time.perf_counter_ns()
    norm_filters = _normalize_filters(filters)

    if hasattr(conn, "read"):
        # Pool path: check out a single reader for the duration of
        # the call. Under concurrency, extra requests queue on the
        # pool's asyncio.Queue — which is correct, because SQLite
        # itself won't actually parallelize them anyway.
        async with conn.read() as reader:  # type: ignore[union-attr]
            return await _search_impl(
                reader, norm_filters, query, ts_start, ts_end, cursor, limit, t0
            )
    return await _search_impl(
        conn,  # type: ignore[arg-type]
        norm_filters,
        query,
        ts_start,
        ts_end,
        cursor,
        limit,
        t0,
    )


async def facets_only(
    conn: DbLike,
    filters: Optional[Mapping[str, List[Any]]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
) -> FacetsOnlyResponse:
    """Run only the facet UNION ALL and return the summaries.

    Used by ``GET /api/facets`` where the client only wants counts
    (e.g. to repopulate a sidebar after an external event). Accepts
    the same pool-or-conn union as ``search``.
    """
    t0 = time.perf_counter_ns()
    norm_filters = _normalize_filters(filters)

    if hasattr(conn, "read"):
        async with conn.read() as reader:  # type: ignore[union-attr]
            return await _facets_only_impl(
                reader, norm_filters, query, ts_start, ts_end, t0
            )
    return await _facets_only_impl(
        conn,  # type: ignore[arg-type]
        norm_filters,
        query,
        ts_start,
        ts_end,
        t0,
    )
