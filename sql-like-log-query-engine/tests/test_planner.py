from __future__ import annotations

import json
import statistics
import time

import pytest

from src.parser import parse_sql
from src.planner import QueryPlanner, render_plan_text
from src.planner.planner import serialize_ast
from src.shared import ast


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _plan_sql(sql: str, partitions):
    node = parse_sql(sql)
    return QueryPlanner(partitions).plan(node), node


def _prune_step(plan):
    steps = [s for s in plan.steps if s.op == "prune"]
    assert len(steps) == 1, "planner must emit exactly one prune step"
    return steps[0]


def _partition_steps(plan):
    return [s for s in plan.steps if s.partition_id is not None]


def _note_for_prefix(plan, prefix: str):
    for note in plan.optimization_notes:
        if note.startswith(prefix):
            return note
    raise AssertionError(f"no optimization note starting with {prefix!r} in {plan.optimization_notes!r}")


# ---------------------------------------------------------------------------
# (1) partition pruning — one test per shape
# ---------------------------------------------------------------------------


def test_no_time_where_keeps_all(sample_partitions):
    plan, _ = _plan_sql("SELECT * FROM logs", sample_partitions)

    prune = _prune_step(plan)
    assert prune.filter == {
        "kept": ["partition-1", "partition-2", "partition-3"],
        "dropped": [],
    }
    assert len(_partition_steps(plan)) == 3
    note = _note_for_prefix(plan, "Partition pruning")
    assert "3/3" in note


def test_time_range_prunes(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE timestamp > '2026-04-15'", sample_partitions
    )

    prune = _prune_step(plan)
    assert prune.filter["kept"] == ["partition-3"]
    assert set(prune.filter["dropped"]) == {"partition-1", "partition-2"}

    parts = _partition_steps(plan)
    assert {s.partition_id for s in parts} == {"partition-3"}

    note = _note_for_prefix(plan, "Partition pruning")
    assert "1/3" in note


def test_time_between_prunes(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE ts BETWEEN '2026-04-05' AND '2026-04-10'",
        sample_partitions,
    )

    prune = _prune_step(plan)
    assert set(prune.filter["kept"]) == {"partition-1", "partition-2"}
    assert prune.filter["dropped"] == ["partition-3"]

    note = _note_for_prefix(plan, "Partition pruning")
    assert "2/3" in note


def test_time_equals_prunes(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE timestamp = '2026-04-20'", sample_partitions
    )

    prune = _prune_step(plan)
    assert prune.filter["kept"] == ["partition-3"]
    assert set(prune.filter["dropped"]) == {"partition-1", "partition-2"}


def test_or_disables_pruning(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE timestamp > '2026-04-15' OR level = 'ERROR'",
        sample_partitions,
    )

    prune = _prune_step(plan)
    assert set(prune.filter["kept"]) == {"partition-1", "partition-2", "partition-3"}
    assert prune.filter["dropped"] == []

    note = _note_for_prefix(plan, "Partition pruning")
    assert "3/3" in note


# ---------------------------------------------------------------------------
# (2) predicate pushdown
# ---------------------------------------------------------------------------


def test_predicate_pushdown_attaches_non_time_subtree(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE level = 'ERROR' AND timestamp > '2026-04-15'",
        sample_partitions,
    )

    prune = _prune_step(plan)
    assert prune.filter["kept"] == ["partition-3"]

    parts = _partition_steps(plan)
    assert len(parts) == 1
    filter_step = parts[0]
    assert filter_step.op == "filter"
    assert filter_step.filter is not None
    assert "ast" in filter_step.filter

    # The remaining AST should contain the level='ERROR' predicate...
    serialized = filter_step.filter["ast"]
    # must be valid JSON (no tuples / datetimes)
    json.dumps(serialized)

    text = json.dumps(serialized)
    assert '"level"' in text
    assert "ERROR" in text
    # ... but NOT the timestamp > predicate.
    assert "timestamp" not in text.lower()

    # And the pushdown note must be present.
    notes = plan.optimization_notes
    assert any("Predicate pushdown" in n for n in notes)


def test_pushdown_reduces_to_none_when_only_time_filter(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE timestamp > '2026-04-15'", sample_partitions
    )
    parts = _partition_steps(plan)
    assert parts, "expected at least one partition step"
    # After stripping the solitary time predicate, no filter should remain.
    assert parts[0].filter is None


def test_serialize_ast_handles_all_node_kinds():
    node = parse_sql(
        "SELECT * FROM logs "
        "WHERE level IN ('ERROR', 'WARN') "
        "AND duration_ms BETWEEN 100 AND 500 "
        "AND message CONTAINS 'timeout' "
        "AND NOT (status_code = 500)"
    )
    blob = serialize_ast(node.where)
    # Round-trips through JSON unchanged.
    json.dumps(blob)

    # Sanity: top-level is an AND binop.
    assert blob["kind"] == "binop"
    assert blob["op"] == "AND"


# ---------------------------------------------------------------------------
# (3) aggregation distribution
# ---------------------------------------------------------------------------


def test_aggregation_distribution_count(sample_partitions):
    plan, _ = _plan_sql("SELECT COUNT(*) FROM logs", sample_partitions)

    partial = [s for s in plan.steps if s.op == "partial_aggregate"]
    global_agg = [s for s in plan.steps if s.op == "global_aggregate"]

    assert len(partial) == 3, "one partial_aggregate per kept partition"
    assert len(global_agg) == 1
    assert global_agg[0].partition_id is None

    # No plain `gather` step in aggregation plans.
    assert not any(s.op == "gather" for s in plan.steps)

    for step in partial:
        funcs = step.aggregation["functions"]
        assert ["COUNT", "*"] in funcs
        assert step.aggregation["group_by"] == []

    note = _note_for_prefix(plan, "Aggregation distribution")
    assert "Local + global" in note


def test_group_by_only(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT service, COUNT(*) FROM logs GROUP BY service", sample_partitions
    )

    partial = [s for s in plan.steps if s.op == "partial_aggregate"]
    global_agg = [s for s in plan.steps if s.op == "global_aggregate"]

    assert len(partial) == 3
    assert len(global_agg) == 1

    for step in partial:
        assert step.aggregation["group_by"] == ["service"]
        assert ["COUNT", "*"] in step.aggregation["functions"]

    assert global_agg[0].aggregation["group_by"] == ["service"]


def test_combined_where_and_group_by(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT level, COUNT(*) FROM logs "
        "WHERE timestamp > '2026-04-08' GROUP BY level",
        sample_partitions,
    )

    prune = _prune_step(plan)
    assert set(prune.filter["kept"]) == {"partition-2", "partition-3"}

    partial = [s for s in plan.steps if s.op == "partial_aggregate"]
    global_agg = [s for s in plan.steps if s.op == "global_aggregate"]

    assert len(partial) == 2  # one per surviving partition
    assert len(global_agg) == 1

    # Each partial step has the aggregation spec but no remaining filter
    # (time predicate was the only WHERE; it's been stripped).
    for step in partial:
        assert step.aggregation["group_by"] == ["level"]
        assert ["COUNT", "*"] in step.aggregation["functions"]
        assert step.filter is None

    assert global_agg[0].aggregation["group_by"] == ["level"]


def test_combined_where_and_group_by_retains_non_time_filter(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT level, COUNT(*) FROM logs "
        "WHERE timestamp > '2026-04-08' AND service = 'api' "
        "GROUP BY level",
        sample_partitions,
    )

    partial = [s for s in plan.steps if s.op == "partial_aggregate"]
    assert len(partial) == 2

    for step in partial:
        assert step.filter is not None
        assert "ast" in step.filter
        blob = json.dumps(step.filter["ast"])
        assert "service" in blob
        assert "api" in blob
        assert "timestamp" not in blob.lower()


# ---------------------------------------------------------------------------
# (4) cost, parallelism, and non-aggregation gather step
# ---------------------------------------------------------------------------


def test_non_aggregation_plan_terminates_with_gather(sample_partitions):
    plan, _ = _plan_sql("SELECT * FROM logs", sample_partitions)
    # prune + 3 filter + gather
    assert plan.steps[-1].op == "gather"
    assert plan.steps[-1].partition_id is None


def test_cost_and_parallelism(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT * FROM logs WHERE level = 'ERROR'", sample_partitions
    )

    expected_cost = sum(step.estimated_cost for step in plan.steps)
    assert plan.total_cost == pytest.approx(expected_cost)

    # 3 partitions touched (no time filter).
    touched = {s.partition_id for s in plan.steps if s.partition_id is not None}
    assert plan.parallelism == len(touched) == 3


def test_render_plan_text_contains_expected_bits(sample_partitions):
    plan, _ = _plan_sql(
        "SELECT service, COUNT(*) FROM logs "
        "WHERE timestamp > '2026-04-15' GROUP BY service",
        sample_partitions,
    )
    text = render_plan_text(plan)

    assert "Execution plan" in text
    assert "parallelism level" in text
    assert "Optimizations applied" in text
    assert "Partition pruning: 1/3" in text
    assert "Predicate pushdown" in text
    assert "Aggregation distribution" in text


# ---------------------------------------------------------------------------
# (5) performance
# ---------------------------------------------------------------------------


def test_plan_perf(sample_partitions):
    sql = (
        "SELECT service, level, COUNT(*) AS n "
        "FROM logs "
        "WHERE level IN ('ERROR', 'WARN') "
        "AND timestamp BETWEEN '2026-04-05' AND '2026-04-18' "
        "AND message CONTAINS 'timeout' "
        "GROUP BY service, level "
        "ORDER BY n DESC "
        "LIMIT 50"
    )
    # Parse once so we isolate planning cost (the parser has its own perf
    # assertion in test_parser.py).
    node = parse_sql(sql)

    durations_ms: list[float] = []
    for _ in range(100):
        start = time.perf_counter()
        QueryPlanner(sample_partitions).plan(node)
        durations_ms.append((time.perf_counter() - start) * 1000.0)

    median = statistics.median(durations_ms)
    assert median <= 50.0, f"median plan build was {median:.2f}ms, exceeds 50ms budget"
