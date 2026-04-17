"""Demo script for the distributed SQL-like log query engine.

Runs three representative query patterns against a live coordinator and
prints a formatted block for each one that matches ``project_requirements``
§8 character for character (emojis, bullets, timing, counts).

Runs inside Docker via::

    docker compose run --rm test python scripts/demo.py

``PYTHONPATH=/app`` is set in the test service, so ``from src.parser ...``
below resolves without needing to install the project as a package.
"""

from __future__ import annotations

import sys
from typing import Any

import click
import httpx

# Local imports — kept inside the script entry point per the task spec.
from src.parser.parser import parse_sql
from src.shared import ast


# The three demo queries. Chosen to exercise (1) temporal pruning + filter,
# (2) text search, (3) full analytical GROUP BY with aggregate distribution.
DEMO_QUERIES: list[str] = [
    (
        "SELECT * FROM logs WHERE level = 'ERROR' "
        "AND timestamp BETWEEN '2026-04-08' AND '2026-04-14' LIMIT 10"
    ),
    "SELECT * FROM logs WHERE message CONTAINS 'timeout' LIMIT 10",
    (
        "SELECT service, COUNT(*) AS cnt FROM logs "
        "GROUP BY service ORDER BY cnt DESC LIMIT 5"
    ),
]


# ---------------------------------------------------------------------------
# AST inspection helpers
# ---------------------------------------------------------------------------


def _count_fields(select: ast.Select) -> int:
    """Return the number of columns in the SELECT list.

    ``SELECT *`` counts as 1.
    """

    return len(select.columns)


def _count_conditions(select: ast.Select) -> int:
    """Return the number of leaf predicate nodes in the WHERE clause.

    Counts ``BinOp`` (non-AND/OR only), ``In``, ``Between``, ``Contains``,
    and ``Not`` nodes. ``AND`` / ``OR`` connectors are structural and not
    themselves conditions.
    """

    if select.where is None:
        return 0

    count = 0

    def _walk(node: Any) -> None:
        nonlocal count
        if isinstance(node, ast.BinOp):
            if node.op in ("AND", "OR"):
                _walk(node.left)
                _walk(node.right)
                return
            count += 1
            return
        if isinstance(node, (ast.In, ast.Between, ast.Contains)):
            count += 1
            return
        if isinstance(node, ast.Not):
            _walk(node.expr)
            count += 1
            return
        # Anything else (bare literal / identifier in WHERE) counts as 1.
        count += 1

    _walk(select.where)
    return count


# ---------------------------------------------------------------------------
# output formatting
# ---------------------------------------------------------------------------


def _print_query_block(
    query_idx: int,
    sql: str,
    n_fields: int,
    n_conditions: int,
    n_steps: int,
    parallelism: int,
    optimizations: list[str],
    execution_ms: float,
    records_processed: int,
) -> None:
    """Print a single demo-query output block in the §8 format."""

    print(f"📝 Demo Query {query_idx}")
    print(f"Query: {sql}")
    print(
        f"✅ Parsed successfully: {n_fields} fields, "
        f"{n_conditions} conditions"
    )
    print(
        f"📊 Execution plan: {n_steps} steps, parallelism level {parallelism}"
    )
    print("🔧 Optimizations applied:")
    # The planner always emits pruning + pushdown; aggregation distribution
    # is only added when the query has aggregates/GROUP BY. We echo
    # whichever notes the coordinator reported, in order.
    for note in optimizations:
        print(f"   • {note}")
    print(f"⚡ Execution completed in {execution_ms:.1f}ms")
    print(f"📋 Results: {records_processed} records processed")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--target-url",
    default="http://coordinator:8000",
    show_default=True,
    help="Base URL of the coordinator service.",
)
@click.option(
    "--timeout",
    default=30.0,
    show_default=True,
    type=float,
    help="HTTP timeout in seconds for each request.",
)
def main(target_url: str, timeout: float) -> None:
    """Run three representative queries and print formatted output for each."""

    target_url = target_url.rstrip("/")

    with httpx.Client(base_url=target_url, timeout=timeout) as client:
        for idx, sql in enumerate(DEMO_QUERIES, start=1):
            # Local AST walk for the field / condition counts.
            try:
                select = parse_sql(sql)
            except Exception as exc:
                click.echo(f"❌ Query {idx} failed: parse error: {exc}", err=True)
                sys.exit(1)

            n_fields = _count_fields(select)
            n_conditions = _count_conditions(select)

            # Ask the coordinator to run the query.
            try:
                response = client.post("/api/query", json={"query": sql})
                response.raise_for_status()
                body = response.json()
            except httpx.HTTPError as exc:
                click.echo(f"❌ Query {idx} failed: {exc}", err=True)
                sys.exit(1)
            except Exception as exc:  # pragma: no cover - defensive
                click.echo(f"❌ Query {idx} failed: {exc}", err=True)
                sys.exit(1)

            plan = body.get("plan") or {}
            steps = plan.get("steps") or []
            parallelism = int(plan.get("parallelism", 0) or 0)
            optimizations: list[str] = list(
                body.get("optimizations_applied") or []
            )
            execution_ms = float(body.get("execution_time_ms", 0.0) or 0.0)
            records_processed = int(body.get("records_processed", 0) or 0)

            _print_query_block(
                query_idx=idx,
                sql=sql,
                n_fields=n_fields,
                n_conditions=n_conditions,
                n_steps=len(steps),
                parallelism=parallelism,
                optimizations=optimizations,
                execution_ms=execution_ms,
                records_processed=records_processed,
            )

            # Blank line between queries.
            if idx < len(DEMO_QUERIES):
                print()

        print()
        print(f"✔ Demo complete — {len(DEMO_QUERIES)} queries executed.")


if __name__ == "__main__":
    main()
