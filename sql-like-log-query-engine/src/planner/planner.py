from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.shared import ast
from src.shared.models import ExecutionPlan, ExecutionStep, PartitionMetadata

from .plan import AggregationSpec


_TIME_COLS: frozenset[str] = frozenset({"timestamp", "ts"})
_AGG_FUNCS: frozenset[str] = frozenset({"COUNT", "SUM", "AVG", "MIN", "MAX"})
_COMPARISON_OPS: frozenset[str] = frozenset({"=", ">", ">=", "<", "<="})

# Per-step base costs. These intentionally stay simple — real cost modelling
# would factor in indexed-field hits, selectivity, and row-width, but the
# spec explicitly keeps the bar at "estimated".
_PRUNE_COST = 1.0
_FILTER_COST = 100.0
_PARTIAL_AGG_COST = 150.0
_GLOBAL_AGG_COST = 50.0
_GATHER_COST = 20.0


# --- bound tracking ----------------------------------------------------------


@dataclass
class _TimeBound:
    """Effective time constraint distilled from the WHERE clause.

    ``low``/``high`` are closed-interval endpoints (inclusive). ``None`` on a
    side means unbounded. ``valid=False`` signals that we detected some
    structural reason why we can't safely prune (e.g. an OR branch touches
    the time column), in which case the planner must keep every partition.
    """

    low: datetime | None = None
    high: datetime | None = None
    valid: bool = True


# --- planner -----------------------------------------------------------------


class QueryPlanner:
    """Turn a parsed ``Select`` AST into an annotated :class:`ExecutionPlan`.

    The planner applies three classical optimizations:

    1. **Partition pruning** — inspect WHERE for time-range predicates on
       ``timestamp``/``ts`` and drop any partition whose ``time_range``
       cannot intersect the resulting interval.
    2. **Predicate pushdown** — hand the full WHERE expression to each
       surviving partition as a serialized AST, so filtering runs at the
       storage layer. Time predicates are kept in the pushed-down filter
       because a partition's ``time_range`` can partially overlap the
       query bound; the partition executor still needs to apply the
       predicate row-by-row (it uses a timestamp index for efficiency).
    3. **Aggregation distribution** — when the query has aggregate functions
       or a GROUP BY, emit per-partition partial aggregations plus a single
       coordinator-side global aggregation merge step.
    """

    def __init__(self, partitions: list[PartitionMetadata]) -> None:
        # Only healthy partitions participate in planning.
        self.partitions: list[PartitionMetadata] = [p for p in partitions if p.healthy]

    # --- public entry point ---------------------------------------------

    def plan(self, ast_root: ast.Select) -> ExecutionPlan:
        steps: list[ExecutionStep] = []
        notes: list[str] = []

        # ---- (1) Partition pruning -------------------------------------
        bound = _extract_time_bound(ast_root.where)
        kept_partitions, dropped_ids = _apply_pruning(self.partitions, bound)

        total = len(self.partitions)
        kept_ids = [p.id for p in kept_partitions]
        steps.append(
            ExecutionStep(
                op="prune",
                filter={"kept": list(kept_ids), "dropped": list(dropped_ids)},
                estimated_cost=_PRUNE_COST,
            )
        )
        notes.append(
            f"Partition pruning: {len(kept_ids)}/{total} partitions selected"
        )

        # ---- (2) Predicate pushdown ------------------------------------
        # Push the full WHERE subtree down to every surviving partition.
        # Partition pruning at the coordinator is only an optimization: when
        # a partition's time_range *partially* overlaps the query bound, the
        # time predicate still has to be evaluated per-row at the partition
        # (the partition executor already handles timestamp comparisons, and
        # uses a timestamp index for efficiency). Stripping time predicates
        # here was incorrect and let out-of-bound rows leak through.
        pushed_filter: dict[str, Any] | None = (
            {"ast": serialize_ast(ast_root.where)}
            if ast_root.where is not None
            else None
        )
        notes.append("Predicate pushdown: WHERE conditions pushed to storage layer")

        # ---- (3) Aggregation distribution ------------------------------
        agg_spec = _detect_aggregation(ast_root)
        has_aggregation = bool(agg_spec.functions) or bool(agg_spec.group_by)

        if has_aggregation:
            agg_payload = {
                "functions": [list(f) for f in agg_spec.functions],
                "group_by": list(agg_spec.group_by),
            }
            for part in kept_partitions:
                step_kwargs: dict[str, Any] = {
                    "op": "partial_aggregate",
                    "partition_id": part.id,
                    "aggregation": dict(agg_payload),
                    "estimated_cost": _PARTIAL_AGG_COST,
                }
                if pushed_filter is not None:
                    step_kwargs["filter"] = dict(pushed_filter)
                steps.append(ExecutionStep(**step_kwargs))

            steps.append(
                ExecutionStep(
                    op="global_aggregate",
                    partition_id=None,
                    aggregation=dict(agg_payload),
                    estimated_cost=_GLOBAL_AGG_COST,
                )
            )
            notes.append(
                "Aggregation distribution: Local + global aggregation strategy"
            )
        else:
            for part in kept_partitions:
                step_kwargs = {
                    "op": "filter",
                    "partition_id": part.id,
                    "estimated_cost": _FILTER_COST,
                }
                if pushed_filter is not None:
                    step_kwargs["filter"] = dict(pushed_filter)
                steps.append(ExecutionStep(**step_kwargs))

            steps.append(
                ExecutionStep(
                    op="gather",
                    partition_id=None,
                    estimated_cost=_GATHER_COST,
                )
            )

        total_cost = sum(step.estimated_cost for step in steps)
        parallelism = max(len(kept_partitions), 1)

        return ExecutionPlan(
            steps=steps,
            total_cost=total_cost,
            parallelism=parallelism,
            optimization_notes=notes,
        )


# --- AST serialization -------------------------------------------------------


def serialize_ast(expr: ast.Expr | None) -> dict[str, Any] | None:
    """Walk an AST expression and produce a JSON-safe dict.

    Each node carries a ``"kind"`` tag so the partition-side executor (later)
    can re-dispatch without needing the Python dataclasses. No tuples or
    datetimes are emitted — only str/int/float/bool/list/dict/None.
    """

    if expr is None:
        return None

    if isinstance(expr, ast.Identifier):
        return {"kind": "identifier", "name": expr.name}

    if isinstance(expr, ast.StringLit):
        return {"kind": "string", "value": expr.value}

    if isinstance(expr, ast.NumberLit):
        return {"kind": "number", "value": expr.value}

    if isinstance(expr, ast.BoolLit):
        return {"kind": "bool", "value": expr.value}

    if isinstance(expr, ast.Star):
        return {"kind": "star"}

    if isinstance(expr, ast.FuncCall):
        return {
            "kind": "func_call",
            "name": expr.name,
            "args": [serialize_ast(a) for a in expr.args],
        }

    if isinstance(expr, ast.BinOp):
        return {
            "kind": "binop",
            "op": expr.op,
            "left": serialize_ast(expr.left),
            "right": serialize_ast(expr.right),
        }

    if isinstance(expr, ast.In):
        return {
            "kind": "in",
            "field": serialize_ast(expr.field),
            "values": [serialize_ast(v) for v in expr.values],
        }

    if isinstance(expr, ast.Between):
        return {
            "kind": "between",
            "field": serialize_ast(expr.field),
            "low": serialize_ast(expr.low),
            "high": serialize_ast(expr.high),
        }

    if isinstance(expr, ast.Contains):
        return {
            "kind": "contains",
            "field": serialize_ast(expr.field),
            "needle": serialize_ast(expr.needle),
        }

    if isinstance(expr, ast.Not):
        return {"kind": "not", "expr": serialize_ast(expr.expr)}

    raise TypeError(f"unsupported AST node for serialization: {type(expr).__name__}")


# --- time-bound extraction ---------------------------------------------------


def _is_time_ident(expr: ast.Expr) -> bool:
    return (
        isinstance(expr, ast.Identifier) and expr.name.lower() in _TIME_COLS
    )


def _parse_time_literal(expr: ast.Expr) -> datetime | None:
    """Best-effort convert a literal on the RHS of a time predicate to a
    naive UTC datetime. Returns ``None`` if the value can't be interpreted
    (in which case the caller should fall back to "keep all partitions")."""

    if isinstance(expr, ast.StringLit):
        raw = expr.value.strip()
        if not raw:
            return None
        # Accept trailing Z by normalising to +00:00 (datetime.fromisoformat
        # in 3.12 does accept Z, but normalising is safer across minor
        # versions of Python).
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    if isinstance(expr, ast.NumberLit):
        try:
            return datetime.utcfromtimestamp(float(expr.value))
        except (OverflowError, OSError, ValueError):
            return None

    return None


def _intersect_bound(current: _TimeBound, new: _TimeBound) -> _TimeBound:
    """Tighten ``current`` by AND-combining it with ``new``.

    An invalid input always produces an invalid result (it poisons the AND
    chain, same as when the caller discovers an OR sub-branch)."""

    if not current.valid or not new.valid:
        return _TimeBound(valid=False)

    low = current.low
    if new.low is not None:
        low = new.low if low is None else max(low, new.low)

    high = current.high
    if new.high is not None:
        high = new.high if high is None else min(high, new.high)

    return _TimeBound(low=low, high=high, valid=True)


def _leaf_time_bound(expr: ast.Expr) -> _TimeBound | None:
    """Return a :class:`_TimeBound` for a single predicate if that predicate
    references a time column, else ``None``. Returning ``None`` means "this
    leaf doesn't constrain time, skip it (keeps whatever bound we already
    have)"."""

    if isinstance(expr, ast.BinOp) and expr.op in _COMPARISON_OPS:
        left, right = expr.left, expr.right
        # Only handle the (ident OP literal) form — flipping is rare and the
        # parser never produces ``'2026-01-01' < ts`` because numbers and
        # strings never appear on the left of comparisons in practice.
        if _is_time_ident(left):
            ts = _parse_time_literal(right)
            if ts is None:
                # Literal shape we can't interpret — be conservative.
                return _TimeBound(valid=False)
            if expr.op == "=":
                return _TimeBound(low=ts, high=ts, valid=True)
            if expr.op == ">":
                return _TimeBound(low=ts, high=None, valid=True)
            if expr.op == ">=":
                return _TimeBound(low=ts, high=None, valid=True)
            if expr.op == "<":
                return _TimeBound(low=None, high=ts, valid=True)
            if expr.op == "<=":
                return _TimeBound(low=None, high=ts, valid=True)
        return None

    if isinstance(expr, ast.Between) and _is_time_ident(expr.field):
        low = _parse_time_literal(expr.low)
        high = _parse_time_literal(expr.high)
        if low is None or high is None:
            return _TimeBound(valid=False)
        return _TimeBound(low=low, high=high, valid=True)

    if isinstance(expr, ast.In) and _is_time_ident(expr.field):
        # Treat IN as the span from min(values) to max(values).
        parsed: list[datetime] = []
        for v in expr.values:
            ts = _parse_time_literal(v)
            if ts is None:
                return _TimeBound(valid=False)
            parsed.append(ts)
        if not parsed:
            return _TimeBound(valid=False)
        return _TimeBound(low=min(parsed), high=max(parsed), valid=True)

    return None


def _extract_time_bound(where: ast.Expr | None) -> _TimeBound:
    """Collapse an AND-tree WHERE into a single effective time bound.

    - ``None`` WHERE → empty bound (keep all).
    - Any time-touching node reached under an OR → mark invalid (keep all).
    - Every other branch is traversed and intersected.
    """

    if where is None:
        return _TimeBound()

    if isinstance(where, ast.BinOp) and where.op == "AND":
        left = _extract_time_bound(where.left)
        right = _extract_time_bound(where.right)
        return _intersect_bound(left, right)

    if isinstance(where, ast.BinOp) and where.op == "OR":
        # Conservative: if either side references time, we can't prune.
        if _expression_touches_time(where.left) or _expression_touches_time(where.right):
            return _TimeBound(valid=False)
        return _TimeBound()

    if isinstance(where, ast.Not):
        # A negated time predicate could match anything outside our bound.
        if _expression_touches_time(where.expr):
            return _TimeBound(valid=False)
        return _TimeBound()

    leaf = _leaf_time_bound(where)
    if leaf is not None:
        return leaf
    return _TimeBound()


def _expression_touches_time(expr: ast.Expr | None) -> bool:
    """True iff ``expr`` has any leaf predicate on a time column."""

    if expr is None:
        return False
    if isinstance(expr, ast.BinOp) and expr.op in {"AND", "OR"}:
        return _expression_touches_time(expr.left) or _expression_touches_time(
            expr.right
        )
    if isinstance(expr, ast.Not):
        return _expression_touches_time(expr.expr)
    if isinstance(expr, ast.BinOp):
        return _is_time_ident(expr.left)
    if isinstance(expr, ast.Between):
        return _is_time_ident(expr.field)
    if isinstance(expr, ast.In):
        return _is_time_ident(expr.field)
    if isinstance(expr, ast.Contains):
        return _is_time_ident(expr.field)
    return False


# --- pruning ----------------------------------------------------------------


def _apply_pruning(
    partitions: list[PartitionMetadata], bound: _TimeBound
) -> tuple[list[PartitionMetadata], list[str]]:
    """Return (kept, dropped_ids) given the collapsed time bound."""

    # Invalid bound (OR-branch, un-parseable literal, …) ⇒ keep everything.
    if not bound.valid:
        return list(partitions), []

    # No actual constraint ⇒ keep everything.
    if bound.low is None and bound.high is None:
        return list(partitions), []

    kept: list[PartitionMetadata] = []
    dropped: list[str] = []
    for part in partitions:
        p_start = _strip_tz(part.time_range.start)
        p_end = _strip_tz(part.time_range.end)

        # Intersection test. The partition's [p_start, p_end] must intersect
        # the constraint's [bound.low or -inf, bound.high or +inf].
        if bound.low is not None and bound.low > p_end:
            dropped.append(part.id)
            continue
        if bound.high is not None and bound.high < p_start:
            dropped.append(part.id)
            continue
        kept.append(part)

    return kept, dropped


def _strip_tz(dt: datetime) -> datetime:
    """Normalize a datetime to naive UTC for comparison.

    Pydantic's default parsing of ISO-8601 without zone yields naive
    datetimes, which is what the fixtures use. Any tz-aware value gets
    converted to UTC and stripped so comparisons with naive bounds remain
    valid."""

    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# --- aggregation detection --------------------------------------------------


def _detect_aggregation(select: ast.Select) -> AggregationSpec:
    """Return the (functions, group_by) intent for this query."""

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
    return AggregationSpec(functions=functions, group_by=group_by)
