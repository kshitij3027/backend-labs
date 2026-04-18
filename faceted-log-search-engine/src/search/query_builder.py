"""SQL assembly for faceted search over the ``logs`` table.

This module is the pure, test-friendly core of the search pipeline.
It imports nothing from the database layer — every function returns
a ``(sql, params)`` tuple that the caller executes against an
``aiosqlite.Connection``.

Three primitives live here:

1. ``escape_like`` — escape user input for a LIKE ... ESCAPE '\' match.
2. ``build_where`` — build the WHERE clause body for the active filter
   set, with an optional ``skip`` dimension so facet counts can exclude
   their own predicate (the "excluded-self" trick).
3. ``build_facet_sql`` / ``build_results_sql`` — stitch per-dimension
   UNION ALL subqueries and the keyset-paginated results query,
   respectively.

Every user-supplied value is bound via ``?`` placeholders; strings are
never interpolated into SQL. ``FACET_DIMS`` is the single source of
truth for the set of facet dimensions and their iteration order — the
facet response always contains entries in this order.
"""

from __future__ import annotations

from typing import Any, Iterable, List, Mapping, Optional, Tuple


# ---------------------------------------------------------------------------
# Canonical facet dimensions. Order is stable so the UNION ALL facet
# query and the response ``facets`` array are both deterministic.
# ---------------------------------------------------------------------------

FACET_DIMS: tuple[str, ...] = (
    "service",
    "level",
    "region",
    "latency_bucket",
    "hour_bucket",
)

# Columns pulled from ``logs`` for the results projection. Ordered to
# mirror the positional binding inside ``build_results_sql`` and the
# row-to-dict shaping inside ``facet_counter.search``.
RESULT_COLUMNS: tuple[str, ...] = (
    "id",
    "ts",
    "service",
    "level",
    "region",
    "response_time_ms",
    "source_ip",
    "request_id",
    "message",
    "metadata",
    "latency_bucket",
    "hour_bucket",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def escape_like(term: str) -> str:
    """Escape a user term for a SQL ``LIKE ? ESCAPE '\\'`` match.

    Order matters: escape the backslash first, then ``%`` and ``_``,
    otherwise we'd double-escape the backslashes we just inserted.
    The returned value is the inner pattern only — callers wrap with
    ``%...%`` themselves so they can control prefix/suffix semantics.
    """
    return (
        term.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def _coerce(dim: str, val: Any) -> Any:
    """Coerce a filter value to the right Python type for binding.

    ``hour_bucket`` is an INTEGER column (generated from ``strftime``)
    so values that arrive as strings (from query params or JSON) need
    to be converted. Everything else is coerced to ``str`` to match
    the TEXT columns.
    """
    if dim == "hour_bucket":
        if isinstance(val, bool):  # guard against ``True`` becoming ``1``
            raise ValueError("hour_bucket cannot be a bool")
        return int(val)
    return str(val)


def _is_facet_dim(dim: str) -> bool:
    """Ignore unknown dimensions gracefully instead of 400-ing."""
    return dim in FACET_DIMS


# ---------------------------------------------------------------------------
# WHERE-clause builder shared by facet and results queries.
# ---------------------------------------------------------------------------

def build_where(
    filters: Mapping[str, Iterable[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    skip: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    """Compose the WHERE body (without the keyword ``WHERE``).

    The returned SQL always begins with ``1=1`` so downstream callers
    can unconditionally concatenate ``AND`` clauses on top without
    branching on whether any predicate was actually present.

    ``skip`` allows the facet query to omit a single dimension's own
    predicate so its COUNT reflects sibling values — the core of the
    excluded-self pattern.

    Free-text ``query`` is matched with
    ``message LIKE ? ESCAPE '\\'`` so wildcard characters in user
    input are treated literally. Empty/blank queries are ignored.
    """
    parts: list[str] = ["1=1"]
    params: list[Any] = []

    # Facet IN (...) clauses, ordered by FACET_DIMS for stability.
    for dim in FACET_DIMS:
        if dim == skip:
            continue
        values = filters.get(dim) if filters else None
        if not values:
            continue
        # Only collect real values; empty lists should have been short
        # -circuited above but double-check after coercion.
        coerced: list[Any] = [_coerce(dim, v) for v in values]
        if not coerced:
            continue
        placeholders = ",".join(["?"] * len(coerced))
        parts.append(f"{dim} IN ({placeholders})")
        params.extend(coerced)

    # Silently ignore keys that aren't real facet dimensions (per spec).
    # Nothing to do — the loop above already filtered by FACET_DIMS.

    if ts_start is not None:
        parts.append("ts >= ?")
        params.append(int(ts_start))
    if ts_end is not None:
        parts.append("ts <= ?")
        params.append(int(ts_end))

    if query is not None and query.strip():
        parts.append("message LIKE ? ESCAPE '\\'")
        params.append(f"%{escape_like(query)}%")

    where_body = " AND ".join(parts)
    return where_body, params


# ---------------------------------------------------------------------------
# Facet SQL — UNION ALL of per-dimension GROUP BY with excluded-self.
# ---------------------------------------------------------------------------

def build_facet_sql(
    filters: Mapping[str, Iterable[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
) -> Tuple[str, List[Any]]:
    """Return a single UNION ALL query that computes all facet counts.

    For every dimension ``d`` in ``FACET_DIMS`` we emit::

        SELECT '<d>' AS facet, <d> AS value, COUNT(*) AS c
        FROM logs
        WHERE <full_where_EXCEPT_<d>>
        GROUP BY <d>

    and join the subqueries with ``UNION ALL``. Parameter lists from
    each sub-build are concatenated in the same order so positional
    binding remains correct.
    """
    subqueries: list[str] = []
    all_params: list[Any] = []

    for dim in FACET_DIMS:
        where_body, params = build_where(
            filters=filters,
            query=query,
            ts_start=ts_start,
            ts_end=ts_end,
            skip=dim,
        )
        sub = (
            f"SELECT '{dim}' AS facet, {dim} AS value, COUNT(*) AS c "
            f"FROM logs "
            f"WHERE {where_body} "
            f"GROUP BY {dim}"
        )
        subqueries.append(sub)
        all_params.extend(params)

    sql = "\nUNION ALL\n".join(subqueries)
    return sql, all_params


# ---------------------------------------------------------------------------
# Results SQL — full WHERE + keyset-paginated ORDER BY ts DESC.
# ---------------------------------------------------------------------------

def build_results_sql(
    filters: Mapping[str, Iterable[Any]],
    query: Optional[str],
    ts_start: Optional[int],
    ts_end: Optional[int],
    cursor: Optional[int],
    limit: int,
) -> Tuple[str, List[Any]]:
    """Build the paginated log-rows query.

    Uses ``ts < ?`` keyset pagination (not OFFSET) and fetches
    ``limit + 1`` rows so the caller can detect ``has_more`` without a
    separate COUNT query. Columns are taken from ``RESULT_COLUMNS`` so
    the projection stays in sync with the dict shaping in
    ``facet_counter.search``.
    """
    where_body, params = build_where(
        filters=filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
        skip=None,
    )

    if cursor is not None:
        where_body = f"{where_body} AND ts < ?"
        params.append(int(cursor))

    columns_sql = ", ".join(RESULT_COLUMNS)
    sql = (
        f"SELECT {columns_sql} "
        f"FROM logs "
        f"WHERE {where_body} "
        f"ORDER BY ts DESC "
        f"LIMIT ?"
    )
    params.append(int(limit) + 1)
    return sql, params
