"""Coordinator-side result merge.

The scatter phase yields one :class:`PartitionExecuteResponse` per partition.
This module flattens those into the final result set described by the
user's SELECT:

  * ``SELECT *`` / ``SELECT field_list`` (no aggregates, no GROUP BY):
    concatenate rows, then apply HAVING/ORDER BY/LIMIT/OFFSET (HAVING on
    ungrouped rows means filter rows individually against a boolean AST).

  * Aggregation without GROUP BY: collapse every partition's single
    ``partial_aggregate`` bucket into one result row, with keys taken from
    ``ast.columns`` (respecting aliases).

  * GROUP BY with or without aggregates: union the per-partition group
    buckets, recompute COUNT/SUM/MIN/MAX/AVG per merged bucket, emit one
    row per unique group tuple, then apply HAVING/ORDER BY/LIMIT/OFFSET.
"""

from __future__ import annotations

from typing import Any, Iterable

from src.shared import ast
from src.shared.models import PartitionExecuteResponse


_AGG_FUNCS: frozenset[str] = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def merge(
    partials: list[PartitionExecuteResponse] | list[tuple[str, PartitionExecuteResponse]],
    ast_root: ast.Select,
) -> list[dict]:
    """Merge partition-level results into the final result set.

    ``partials`` may be a plain list of responses or a list of
    ``(partition_id, response)`` tuples (the executor returns the latter).
    Partition id is not needed for merging so both shapes are accepted.
    """

    responses = [p[1] if isinstance(p, tuple) else p for p in partials]

    aggregates, group_by = _detect_aggregation(ast_root)
    has_group_by = bool(group_by)
    has_aggregates = bool(aggregates)

    if not has_aggregates and not has_group_by:
        rows = _concat_rows(responses)
        rows = _project_plain_columns(rows, ast_root)
        rows = _apply_where_like(rows, ast_root.having)  # HAVING on raw rows
        rows = _apply_order_by(rows, ast_root)
        rows = _apply_limit_offset(rows, ast_root)
        return rows

    if has_group_by:
        rows = _merge_grouped(responses, aggregates, group_by, ast_root)
    else:
        rows = _merge_ungrouped(responses, aggregates, ast_root)

    rows = _apply_where_like(rows, ast_root.having)
    rows = _apply_order_by(rows, ast_root)
    rows = _apply_limit_offset(rows, ast_root)
    return rows


# ---------------------------------------------------------------------------
# aggregation detection (local copy so aggregator doesn't import planner)
# ---------------------------------------------------------------------------


def _detect_aggregation(
    select: ast.Select,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Return ``(functions, group_by)`` — mirrors the planner's detection."""

    functions: list[tuple[str, str]] = []
    for col in select.columns:
        expr = col.expr
        if isinstance(expr, ast.FuncCall) and expr.name in _AGG_FUNCS:
            arg_name = "*"
            if expr.args:
                first = expr.args[0]
                if isinstance(first, ast.Identifier):
                    arg_name = first.name
                elif isinstance(first, ast.Star):
                    arg_name = "*"
            functions.append((expr.name, arg_name))

    group_by = [ident.name for ident in select.group_by]
    return functions, group_by


# ---------------------------------------------------------------------------
# no-aggregation path helpers
# ---------------------------------------------------------------------------


def _concat_rows(partials: list[PartitionExecuteResponse]) -> list[dict]:
    out: list[dict] = []
    for p in partials:
        out.extend(p.rows or [])
    return out


def _project_plain_columns(
    rows: list[dict], select: ast.Select
) -> list[dict]:
    """If the query is ``SELECT a, b AS c FROM logs`` (no aggregates), keep
    just those columns (applying aliases). ``SELECT *`` returns rows as-is.
    """

    # SELECT * → pass-through.
    if len(select.columns) == 1 and isinstance(select.columns[0].expr, ast.Star):
        return rows

    projected: list[dict] = []
    for row in rows:
        new_row: dict[str, Any] = {}
        for col in select.columns:
            expr = col.expr
            if isinstance(expr, ast.Identifier):
                key = col.alias or expr.name
                new_row[key] = row.get(expr.name)
            elif isinstance(expr, ast.Star):
                new_row.update(row)
            else:
                # Unexpected (no aggregates in this branch). Fall back to
                # string repr so we never silently drop a column.
                new_row[col.alias or str(expr)] = None
        projected.append(new_row)
    return projected


# ---------------------------------------------------------------------------
# aggregation merge
# ---------------------------------------------------------------------------


def _merge_ungrouped(
    partials: list[PartitionExecuteResponse],
    aggregates: list[tuple[str, str]],
    select: ast.Select,
) -> list[dict]:
    """Collapse ``COUNT/SUM/MIN/MAX/AVG`` across partitions (no GROUP BY)."""

    total_count = 0
    sums: dict[str, float] = {}
    mins: dict[str, Any] = {}
    maxs: dict[str, Any] = {}
    per_col_count: dict[str, int] = {}

    for p in partials:
        agg = p.partial_aggregate or {}
        total_count += int(agg.get("record_count", agg.get("count", 0)) or 0)

        for col, v in (agg.get("sums") or {}).items():
            if v is None:
                continue
            sums[col] = sums.get(col, 0.0) + float(v)
            per_col_count[col] = per_col_count.get(col, 0) + int(
                agg.get("record_count", agg.get("count", 0)) or 0
            )
        for col, v in (agg.get("mins") or {}).items():
            if v is None:
                continue
            mins[col] = v if col not in mins else _min_val(mins[col], v)
        for col, v in (agg.get("maxs") or {}).items():
            if v is None:
                continue
            maxs[col] = v if col not in maxs else _max_val(maxs[col], v)

    # Build one result row with one entry per SELECT column.
    row = _build_agg_row(select, total_count, sums, mins, maxs)
    return [row]


def _merge_grouped(
    partials: list[PartitionExecuteResponse],
    aggregates: list[tuple[str, str]],
    group_by: list[str],
    select: ast.Select,
) -> list[dict]:
    """Union group buckets across partitions, then emit one row per group."""

    buckets: dict[str, dict[str, Any]] = {}

    for p in partials:
        agg = p.partial_aggregate or {}
        groups = agg.get("groups") or {}
        for key, bucket in groups.items():
            merged = buckets.get(key)
            if merged is None:
                merged = {
                    "count": 0,
                    "sums": {},
                    "mins": {},
                    "maxs": {},
                    "group_values": _coerce_group_values(
                        bucket.get("group_values"), group_by
                    ),
                }
                buckets[key] = merged

            merged["count"] += int(bucket.get("count", 0) or 0)

            for col, v in (bucket.get("sums") or {}).items():
                if v is None:
                    continue
                merged["sums"][col] = merged["sums"].get(col, 0.0) + float(v)
            for col, v in (bucket.get("mins") or {}).items():
                if v is None:
                    continue
                existing = merged["mins"].get(col)
                merged["mins"][col] = v if existing is None else _min_val(existing, v)
            for col, v in (bucket.get("maxs") or {}).items():
                if v is None:
                    continue
                existing = merged["maxs"].get(col)
                merged["maxs"][col] = v if existing is None else _max_val(existing, v)

    rows: list[dict] = []
    for bucket in buckets.values():
        total = int(bucket["count"])
        sums = dict(bucket["sums"])
        mins = dict(bucket["mins"])
        maxs = dict(bucket["maxs"])
        group_values = dict(bucket.get("group_values") or {})

        row: dict[str, Any] = {}
        for col in select.columns:
            expr = col.expr
            if isinstance(expr, ast.Identifier) and expr.name in group_values:
                key = col.alias or expr.name
                row[key] = group_values.get(expr.name)
            elif isinstance(expr, ast.FuncCall) and expr.name in _AGG_FUNCS:
                key = col.alias or _agg_display_name(expr)
                row[key] = _compute_agg_value(
                    expr.name,
                    _arg_name(expr),
                    count=total,
                    sums=sums,
                    mins=mins,
                    maxs=maxs,
                    per_col_count={},
                )
            elif isinstance(expr, ast.Identifier):
                # Selecting a field that isn't in the group by — keep a
                # best-effort value so queries like ``SELECT service,
                # COUNT(*) FROM logs GROUP BY service`` keep working even if
                # the partition forgot to echo it.
                key = col.alias or expr.name
                row[key] = group_values.get(expr.name)
            else:
                row[col.alias or "value"] = None
        rows.append(row)

    return rows


def _build_agg_row(
    select: ast.Select,
    total_count: int,
    sums: dict[str, float],
    mins: dict[str, Any],
    maxs: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for col in select.columns:
        expr = col.expr
        if isinstance(expr, ast.FuncCall) and expr.name in _AGG_FUNCS:
            key = col.alias or _agg_display_name(expr)
            row[key] = _compute_agg_value(
                expr.name,
                _arg_name(expr),
                count=total_count,
                sums=sums,
                mins=mins,
                maxs=maxs,
                per_col_count={},
            )
        elif isinstance(expr, ast.Identifier):
            # Plain identifier alongside an aggregate without GROUP BY —
            # not SQL-standard, but don't crash; emit None.
            row[col.alias or expr.name] = None
        else:
            row[col.alias or "value"] = None
    return row


def _compute_agg_value(
    func: str,
    arg: str,
    *,
    count: int,
    sums: dict[str, float],
    mins: dict[str, Any],
    maxs: dict[str, Any],
    per_col_count: dict[str, int],
) -> Any:
    if func == "COUNT":
        return count
    if func == "SUM":
        return sums.get(arg, 0.0)
    if func == "MIN":
        return mins.get(arg)
    if func == "MAX":
        return maxs.get(arg)
    if func == "AVG":
        total = sums.get(arg, 0.0)
        if count <= 0:
            return 0.0
        return total / count
    return None


def _agg_display_name(func: ast.FuncCall) -> str:
    arg = _arg_name(func)
    return f"{func.name}(*)" if arg == "*" else f"{func.name}({arg})"


def _arg_name(func: ast.FuncCall) -> str:
    if not func.args:
        return "*"
    first = func.args[0]
    if isinstance(first, ast.Identifier):
        return first.name
    if isinstance(first, ast.Star):
        return "*"
    return "*"


def _coerce_group_values(
    raw: Any, group_by: list[str]
) -> dict[str, Any]:
    """The partition returns ``group_values`` as either a dict
    (current shape) or — as documented in some specs — a list. Normalise
    to a dict keyed by group-by field name."""

    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, list):
        return {field: raw[i] for i, field in enumerate(group_by) if i < len(raw)}
    return {}


# ---------------------------------------------------------------------------
# HAVING + ORDER BY + LIMIT/OFFSET
# ---------------------------------------------------------------------------


def _apply_where_like(rows: list[dict], expr: ast.Expr | None) -> list[dict]:
    """Filter rows in-memory against a WHERE/HAVING-like expression.

    We implement a small evaluator over the AST here because HAVING runs
    post-aggregation on the coordinator and the local/partition executor
    expects the serialised (dict) form. Keeping the evaluator local avoids
    another serialise→deserialise round trip.
    """

    if expr is None:
        return rows
    return [row for row in rows if _eval_expr(expr, row)]


def _eval_expr(expr: ast.Expr, row: dict) -> Any:
    if isinstance(expr, ast.Identifier):
        return row.get(expr.name)
    if isinstance(expr, ast.StringLit):
        return expr.value
    if isinstance(expr, ast.NumberLit):
        return expr.value
    if isinstance(expr, ast.BoolLit):
        return expr.value
    if isinstance(expr, ast.Star):
        return row
    if isinstance(expr, ast.FuncCall):
        # In HAVING the aggregate already materialised as a column value —
        # look up by its display name.
        key = _agg_display_name(expr)
        return row.get(key)
    if isinstance(expr, ast.BinOp):
        if expr.op == "AND":
            return bool(_eval_expr(expr.left, row)) and bool(
                _eval_expr(expr.right, row)
            )
        if expr.op == "OR":
            return bool(_eval_expr(expr.left, row)) or bool(
                _eval_expr(expr.right, row)
            )
        left = _eval_expr(expr.left, row)
        right = _eval_expr(expr.right, row)
        return _compare(expr.op, left, right)
    if isinstance(expr, ast.In):
        val = _eval_expr(expr.field, row)
        targets = [_eval_expr(v, row) for v in expr.values]
        return val in targets
    if isinstance(expr, ast.Between):
        val = _eval_expr(expr.field, row)
        low = _eval_expr(expr.low, row)
        high = _eval_expr(expr.high, row)
        if val is None or low is None or high is None:
            return False
        try:
            return low <= val <= high
        except TypeError:
            return str(low) <= str(val) <= str(high)
    if isinstance(expr, ast.Contains):
        hay = _eval_expr(expr.field, row)
        needle = expr.needle.value
        return isinstance(hay, str) and isinstance(needle, str) and needle.lower() in hay.lower()
    if isinstance(expr, ast.Not):
        return not bool(_eval_expr(expr.expr, row))
    return None


def _compare(op: str, left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    try:
        if op == "=":
            return _coerce_eq(left, right)
        if op == "!=":
            return not _coerce_eq(left, right)
        if op == "<":
            return _coerce_lt(left, right)
        if op == "<=":
            return _coerce_lt(left, right) or _coerce_eq(left, right)
        if op == ">":
            return _coerce_lt(right, left)
        if op == ">=":
            return _coerce_lt(right, left) or _coerce_eq(left, right)
    except Exception:
        return False
    return False


def _coerce_eq(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return float(a) == float(b)
    try:
        return float(a) == float(b)
    except (TypeError, ValueError):
        return str(a) == str(b)


def _coerce_lt(a: Any, b: Any) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return float(a) < float(b)
    try:
        return float(a) < float(b)
    except (TypeError, ValueError):
        return str(a) < str(b)


def _min_val(a: Any, b: Any) -> Any:
    try:
        return min(a, b)
    except TypeError:
        return min(str(a), str(b))


def _max_val(a: Any, b: Any) -> Any:
    try:
        return max(a, b)
    except TypeError:
        return max(str(a), str(b))


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT / OFFSET
# ---------------------------------------------------------------------------


def _apply_order_by(rows: list[dict], select: ast.Select) -> list[dict]:
    if not select.order_by:
        return rows

    # We support a single ORDER BY item per the parser's grammar.
    item = select.order_by[0]
    field = item.field.name
    reverse = item.direction.upper() == "DESC"

    def _key(row: dict) -> tuple[int, Any]:
        value = row.get(field)
        # Sort None to the end stably.
        if value is None:
            return (1, 0)
        return (0, _sort_value(value))

    return sorted(rows, key=_key, reverse=reverse)


def _sort_value(value: Any) -> Any:
    """Return a sort key that tolerates mixed int/float/str values."""

    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    return (2, str(value))


def _apply_limit_offset(rows: list[dict], select: ast.Select) -> list[dict]:
    offset = select.offset or 0
    limit = select.limit
    if offset:
        rows = rows[offset:]
    if limit is not None:
        rows = rows[:limit]
    return rows


# ---------------------------------------------------------------------------
# convenience exposed for tests / manual use
# ---------------------------------------------------------------------------


def iter_rows(
    partials: Iterable[PartitionExecuteResponse],
) -> Iterable[dict]:  # pragma: no cover - convenience
    for p in partials:
        yield from (p.rows or [])
