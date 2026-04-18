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
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Mapping, Optional

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

async def search(
    conn: aiosqlite.Connection,
    filters: Optional[Mapping[str, List[Any]]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    cursor: Optional[int],
    limit: int,
) -> SearchResponse:
    """Run the results SQL + facet SQL and assemble a ``SearchResponse``.

    Timing covers both queries and the response shaping; network cost
    to SQLite on disk is ignored since aiosqlite runs in-process.
    """
    t0 = time.perf_counter_ns()

    norm_filters = _normalize_filters(filters)
    # Row factory is set to aiosqlite.Row globally via ``connect``; we
    # rely on that for dict-style row access below.

    # --- results ---
    results_sql, results_params = query_builder.build_results_sql(
        filters=norm_filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
        cursor=cursor,
        limit=limit,
    )
    async with conn.execute(results_sql, results_params) as cur:
        rows = await cur.fetchall()

    has_more = len(rows) > limit
    sliced = rows[:limit] if has_more else rows
    logs_out = [_row_to_log_dict(r) for r in sliced]
    next_cursor = int(sliced[-1]["ts"]) if sliced and has_more else None
    total_count = None if has_more else len(sliced)

    # --- facets ---
    facet_sql, facet_params = query_builder.build_facet_sql(
        filters=norm_filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
    )
    async with conn.execute(facet_sql, facet_params) as cur:
        facet_rows = await cur.fetchall()

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


async def facets_only(
    conn: aiosqlite.Connection,
    filters: Optional[Mapping[str, List[Any]]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
) -> FacetsOnlyResponse:
    """Run only the facet UNION ALL and return the summaries.

    Used by ``GET /api/facets`` where the client only wants counts
    (e.g. to repopulate a sidebar after an external event).
    """
    t0 = time.perf_counter_ns()
    norm_filters = _normalize_filters(filters)

    facet_sql, facet_params = query_builder.build_facet_sql(
        filters=norm_filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
    )
    async with conn.execute(facet_sql, facet_params) as cur:
        facet_rows = await cur.fetchall()

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
