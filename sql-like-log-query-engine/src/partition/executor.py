from __future__ import annotations

from typing import Any, Iterable

from .storage import LogStorage


# Sentinel value used by the "could not extract an index-hit" path.
_UNSET = object()


class LocalExecutor:
    """Evaluate a pushed-down filter AST (and optional partial aggregation)
    against the in-memory :class:`LogStorage`.

    The evaluator understands the JSON shape emitted by
    :func:`src.planner.planner.serialize_ast` — dicts keyed by ``"kind"``:
    ``identifier``, ``string``, ``number``, ``bool``, ``star``, ``func_call``,
    ``binop``, ``in``, ``between``, ``contains``, ``not``.

    Index usage strategy
    --------------------
    Before iterating rows we try to narrow the candidate set using the
    storage indexes. Only AND-trees yield narrowing:

    * ``level = 'X'`` / ``service = 'X'`` → intersect with the corresponding
      hash index.
    * ``timestamp OP literal`` → convert to a bisect-based range scan.
    * ``timestamp BETWEEN low AND high`` → ditto.
    * ``timestamp IN (...)`` → union of point lookups.

    Any OR node (or an ``In`` / ``Between`` / ``Contains`` / ``Not`` branch
    we don't know how to specialise) short-circuits the narrowing for the
    subtree it sits under: we simply fall back to "all rows". The full AST
    is evaluated against whichever candidate set survives, so correctness is
    independent of how aggressive the index pass gets.
    """

    def __init__(self, storage: LogStorage) -> None:
        self._storage = storage

    # --- public surface -----------------------------------------------

    @property
    def storage(self) -> LogStorage:
        return self._storage

    def filter(
        self,
        filter_ast: dict | None,
        rows: list[dict] | None = None,
    ) -> list[dict]:
        """Return rows that satisfy ``filter_ast``.

        If ``rows`` is supplied the index narrowing is skipped (the caller
        has already scoped the search space for us) and we evaluate the AST
        directly against that list. Otherwise we try the index pass on
        ``self._storage`` first.
        """

        if rows is not None:
            if filter_ast is None:
                return list(rows)
            return [row for row in rows if self._eval(filter_ast, row)]

        all_rows = self._storage.rows()
        if filter_ast is None:
            return list(all_rows)

        candidate_indices = self._narrow(filter_ast)
        if candidate_indices is None:
            # No narrowing possible — scan everything.
            return [row for row in all_rows if self._eval(filter_ast, row)]

        # Evaluate the full AST against the narrowed candidates (the index
        # pass is only advisory — some predicates inside the narrowed set
        # may still fail the full expression).
        return [
            all_rows[i]
            for i in sorted(candidate_indices)
            if self._eval(filter_ast, all_rows[i])
        ]

    # --- index narrowing ----------------------------------------------

    def _narrow(self, node: dict) -> set[int] | None:
        """Walk an AND-tree and return the intersection of index hits.

        Returns ``None`` if the tree can't be narrowed via indexes (in which
        case the caller must full-scan). An empty set is a legitimate
        answer and means "no rows can match".
        """

        kind = node.get("kind")

        if kind == "binop" and node.get("op") == "AND":
            left = self._narrow(node["left"])
            right = self._narrow(node["right"])
            if left is None and right is None:
                return None
            if left is None:
                return right
            if right is None:
                return left
            return left & right

        if kind == "binop" and node.get("op") == "OR":
            # Could widen by unioning hits, but most of our queries are
            # AND-shaped; keep it simple.
            return None

        if kind == "not":
            return None

        # Leaf nodes.
        leaf_hit = self._leaf_narrow(node)
        return leaf_hit

    def _leaf_narrow(self, node: dict) -> set[int] | None:
        kind = node.get("kind")

        if kind == "binop" and node.get("op") == "=":
            field = _field_name(node.get("left"))
            literal = _literal_value(node.get("right"))
            if field == "level" and isinstance(literal, str):
                return self._storage.filter_by_level(literal)
            if field == "service" and isinstance(literal, str):
                return self._storage.filter_by_service(literal)
            if field == "timestamp" and isinstance(literal, str):
                return self._storage.filter_by_timestamp_range(literal, literal)
            return None

        if kind == "binop" and node.get("op") in (">", ">=", "<", "<="):
            field = _field_name(node.get("left"))
            literal = _literal_value(node.get("right"))
            if field == "timestamp" and isinstance(literal, str):
                op = node["op"]
                if op in (">", ">="):
                    return self._storage.filter_by_timestamp_range(literal, None)
                # < / <=
                return self._storage.filter_by_timestamp_range(None, literal)
            return None

        if kind == "between":
            field = _field_name(node.get("field"))
            low = _literal_value(node.get("low"))
            high = _literal_value(node.get("high"))
            if field == "timestamp" and isinstance(low, str) and isinstance(high, str):
                return self._storage.filter_by_timestamp_range(low, high)
            return None

        if kind == "in":
            field = _field_name(node.get("field"))
            values = [
                _literal_value(v) for v in node.get("values", []) or []
            ]
            if any(v is _UNSET for v in values):
                return None
            if field == "level":
                hits: set[int] = set()
                for val in values:
                    if isinstance(val, str):
                        hits |= self._storage.filter_by_level(val)
                return hits
            if field == "service":
                hits = set()
                for val in values:
                    if isinstance(val, str):
                        hits |= self._storage.filter_by_service(val)
                return hits
            return None

        return None

    # --- full expression evaluator ------------------------------------

    def _eval(self, node: dict | None, row: dict) -> Any:
        if node is None:
            return True

        kind = node.get("kind")

        if kind == "identifier":
            return row.get(node["name"])

        if kind == "string":
            return node["value"]

        if kind == "number":
            return node["value"]

        if kind == "bool":
            return node["value"]

        if kind == "star":
            raise ValueError("Star node cannot be evaluated in a filter expression")

        if kind == "func_call":
            # Aggregate functions don't appear in WHERE — guard anyway.
            raise ValueError(
                f"function call '{node.get('name')}' is not supported in filter"
            )

        if kind == "binop":
            op = node["op"]
            if op == "AND":
                return bool(self._eval(node["left"], row)) and bool(
                    self._eval(node["right"], row)
                )
            if op == "OR":
                return bool(self._eval(node["left"], row)) or bool(
                    self._eval(node["right"], row)
                )
            left = self._eval(node["left"], row)
            right = self._eval(node["right"], row)
            return _compare(op, left, right)

        if kind == "in":
            field = node["field"]
            left = self._eval(field, row)
            targets = [self._eval(v, row) for v in node.get("values", []) or []]
            return _in(left, targets)

        if kind == "between":
            field = node["field"]
            left = self._eval(field, row)
            low = self._eval(node["low"], row)
            high = self._eval(node["high"], row)
            if left is None:
                return False
            return _le(low, left) and _le(left, high)

        if kind == "contains":
            field = node["field"]
            haystack = self._eval(field, row)
            needle = self._eval(node["needle"], row)
            if not isinstance(haystack, str) or not isinstance(needle, str):
                return False
            return needle.lower() in haystack.lower()

        if kind == "not":
            return not bool(self._eval(node["expr"], row))

        raise ValueError(f"unknown AST kind: {kind!r}")

    # --- aggregation --------------------------------------------------

    def partial_aggregate(
        self, rows: list[dict], aggregation: dict | None
    ) -> dict:
        """Compute partial aggregates in the planner's on-the-wire shape.

        ``aggregation`` — as emitted by the planner — is of the form::

            {"functions": [["COUNT", "*"], ["SUM", "duration_ms"], ...],
             "group_by": ["service", "level"]}

        Returns a JSON-safe dict. When ``group_by`` is empty, the result
        looks like::

            {
                "groups": None,
                "aggregates": {"COUNT(*)": n, "SUM(duration_ms)": x, ...},
                "record_count": n,
                "functions": [...],
                "group_by": [],
            }

        With a non-empty ``group_by``, ``groups`` is a dict keyed by the
        joined group tuple (separator = ``\\u0001``, which is safe because
        service/level values don't contain control characters)::

            {
                "groups": {
                    "api\\u0001ERROR": {
                        "count": n,
                        "sums": {"duration_ms": x},
                        "mins": {"duration_ms": y},
                        "maxs": {"duration_ms": z},
                    },
                    ...
                },
                "aggregates": None,
                "record_count": total_rows,
                "functions": [...],
                "group_by": [...],
            }
        """

        if aggregation is None:
            return {}

        functions_raw: Iterable[Any] = aggregation.get("functions", []) or []
        functions: list[tuple[str, str]] = []
        for entry in functions_raw:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            functions.append((str(entry[0]).upper(), str(entry[1])))

        group_by: list[str] = list(aggregation.get("group_by", []) or [])

        # Columns we need SUM/MIN/MAX on — COUNT doesn't need numeric access.
        numeric_cols = sorted(
            {col for func, col in functions if func in ("SUM", "AVG", "MIN", "MAX")}
        )

        record_count = len(rows)

        if not group_by:
            # ungrouped aggregation
            agg: dict[str, Any] = {}
            for func, col in functions:
                key = f"{func}(*)" if col == "*" else f"{func}({col})"
                if func == "COUNT":
                    if col == "*":
                        agg[key] = len(rows)
                    else:
                        agg[key] = sum(1 for r in rows if r.get(col) is not None)
                else:
                    values = [
                        _as_number(r.get(col))
                        for r in rows
                        if r.get(col) is not None
                    ]
                    values = [v for v in values if v is not None]
                    if func == "SUM":
                        agg[key] = sum(values)
                    elif func == "AVG":
                        # The coordinator merges SUM/COUNT → AVG, but for
                        # clients using partial_aggregate directly we still
                        # expose a per-partition AVG.
                        agg[key] = (sum(values) / len(values)) if values else 0.0
                    elif func == "MIN":
                        agg[key] = min(values) if values else None
                    elif func == "MAX":
                        agg[key] = max(values) if values else None

            # Convenience: also surface raw sum/min/max maps so the merge
            # step on the coordinator doesn't have to re-parse the keys.
            sums = {
                col: sum(
                    v for v in (_as_number(r.get(col)) for r in rows) if v is not None
                )
                for col in numeric_cols
            }
            mins = {
                col: _safe_min(
                    v for v in (_as_number(r.get(col)) for r in rows) if v is not None
                )
                for col in numeric_cols
            }
            maxs = {
                col: _safe_max(
                    v for v in (_as_number(r.get(col)) for r in rows) if v is not None
                )
                for col in numeric_cols
            }

            return {
                "groups": None,
                "aggregates": agg,
                "record_count": record_count,
                "count": record_count,
                "sums": sums,
                "mins": mins,
                "maxs": maxs,
                "functions": [list(f) for f in functions],
                "group_by": [],
            }

        # Grouped path ----------------------------------------------------
        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            key_parts = [str(row.get(field, "")) for field in group_by]
            key = "\u0001".join(key_parts)
            bucket = groups.get(key)
            if bucket is None:
                bucket = {
                    "count": 0,
                    "sums": {col: 0.0 for col in numeric_cols},
                    "mins": {col: None for col in numeric_cols},
                    "maxs": {col: None for col in numeric_cols},
                    # Preserve the original group column values so the
                    # coordinator can reconstruct result rows without having
                    # to parse the key back out.
                    "group_values": {field: row.get(field) for field in group_by},
                }
                groups[key] = bucket
            bucket["count"] += 1
            for col in numeric_cols:
                val = _as_number(row.get(col))
                if val is None:
                    continue
                bucket["sums"][col] = bucket["sums"][col] + val
                current_min = bucket["mins"][col]
                bucket["mins"][col] = val if current_min is None else min(
                    current_min, val
                )
                current_max = bucket["maxs"][col]
                bucket["maxs"][col] = val if current_max is None else max(
                    current_max, val
                )

        return {
            "groups": groups,
            "aggregates": None,
            "record_count": record_count,
            "functions": [list(f) for f in functions],
            "group_by": group_by,
        }


# ---------------------------------------------------------------------------
# tiny value helpers
# ---------------------------------------------------------------------------


def _field_name(node: dict | None) -> str | None:
    if isinstance(node, dict) and node.get("kind") == "identifier":
        name = node.get("name")
        return name if isinstance(name, str) else None
    return None


def _literal_value(node: dict | None) -> Any:
    if not isinstance(node, dict):
        return _UNSET
    kind = node.get("kind")
    if kind in ("string", "number", "bool"):
        return node.get("value")
    return _UNSET


def _as_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _safe_min(values: Iterable[float]) -> float | None:
    first = True
    m: float | None = None
    for v in values:
        if first or v < m:  # type: ignore[operator]
            m = v
            first = False
    return m


def _safe_max(values: Iterable[float]) -> float | None:
    first = True
    m: float | None = None
    for v in values:
        if first or v > m:  # type: ignore[operator]
            m = v
            first = False
    return m


def _compare(op: str, left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    lhs, rhs = _coerce_pair(left, right)
    if op == "=":
        return lhs == rhs
    if op == "!=":
        return lhs != rhs
    if op == "<":
        return lhs < rhs
    if op == "<=":
        return lhs <= rhs
    if op == ">":
        return lhs > rhs
    if op == ">=":
        return lhs >= rhs
    raise ValueError(f"unsupported binop: {op!r}")


def _le(a: Any, b: Any) -> bool:
    """Tolerant ``a <= b`` used by BETWEEN."""

    if a is None or b is None:
        return False
    lhs, rhs = _coerce_pair(a, b)
    return lhs <= rhs


def _in(value: Any, targets: list[Any]) -> bool:
    for t in targets:
        try:
            lhs, rhs = _coerce_pair(value, t)
        except Exception:  # pragma: no cover - defensive
            continue
        if lhs == rhs:
            return True
    return False


def _coerce_pair(a: Any, b: Any) -> tuple[Any, Any]:
    """Best-effort numeric coercion so ``'500' = 500`` matches.

    Rules:
    - If both sides are numeric (int/float/bool), compare as floats.
    - If one side is numeric and the other is a numeric-looking string,
      compare as floats.
    - Otherwise compare as strings.
    """

    if isinstance(a, bool) or isinstance(b, bool):
        # Bools compare as themselves to avoid ``True == 1`` surprises.
        return a, b

    a_num = a if isinstance(a, (int, float)) else _try_float(a)
    b_num = b if isinstance(b, (int, float)) else _try_float(b)

    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return float(a), float(b)

    if isinstance(a, (int, float)) and b_num is not None:
        return float(a), float(b_num)

    if isinstance(b, (int, float)) and a_num is not None:
        return float(a_num), float(b)

    # Fall back to string comparison so ISO timestamps still sort sanely.
    return str(a), str(b)


def _try_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
