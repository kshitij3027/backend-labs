from __future__ import annotations

from dataclasses import dataclass, field

from src.shared.models import ExecutionPlan, ExecutionStep

__all__ = ["AggregationSpec", "ExecutionPlan", "ExecutionStep"]


@dataclass
class AggregationSpec:
    """Internal helper describing aggregation intent for a partition.

    This is what the planner hands to the partition executor (in a later
    commit) so that each partition can compute its partial aggregates and
    the coordinator can merge them globally.

    - ``functions`` is an ordered list of ``(FUNC_NAME, column)`` pairs where
      ``column`` is either a field name (e.g. ``duration_ms``) or ``"*"`` for
      ``COUNT(*)``.
    - ``group_by`` is the list of grouping column names; empty if the query
      has no GROUP BY.
    """

    functions: list[tuple[str, str]] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
