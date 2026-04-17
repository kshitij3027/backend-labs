from __future__ import annotations

from src.shared.models import ExecutionPlan, ExecutionStep


def render_plan_text(plan: ExecutionPlan) -> str:
    """Produce the demo-style human-readable block for an execution plan.

    The output follows the format shown in ``project_requirements.md`` §8:
    a header with step count and parallelism, a bulleted list of
    optimizations, and an enumeration of the individual steps with a short
    summary for each one.
    """

    lines: list[str] = []

    lines.append(
        f"📊 Execution plan: {len(plan.steps)} steps, parallelism level {plan.parallelism}"
    )

    # Optimization bullets (cap at 3 per spec example).
    if plan.optimization_notes:
        lines.append("🔧 Optimizations applied:")
        for note in plan.optimization_notes[:3]:
            lines.append(f"   • {note}")

    # Step enumeration.
    if plan.steps:
        lines.append("Steps:")
        n_partitions_in_plan = sum(
            1 for s in plan.steps if s.partition_id is not None
        )
        for idx, step in enumerate(plan.steps, start=1):
            summary = _summarize_step(step, n_partitions_in_plan)
            lines.append(f"   {idx}. [{step.op}] {summary}")

    lines.append(f"💰 Estimated cost: {plan.total_cost:.1f}")

    return "\n".join(lines)


def _summarize_step(step: ExecutionStep, n_partitions_in_plan: int) -> str:
    """Return a concise one-line summary for a single execution step."""

    if step.op == "prune":
        if step.filter is None:
            return "partition pruning"
        kept = step.filter.get("kept", []) or []
        dropped = step.filter.get("dropped", []) or []
        return f"kept {len(kept)} partition(s), dropped {len(dropped)}"

    if step.op == "filter":
        target = step.partition_id or "unknown"
        return f"{target}"

    if step.op == "partial_aggregate":
        target = step.partition_id or "unknown"
        funcs = (step.aggregation or {}).get("functions") or []
        fn_text = ", ".join(f"{name}({col})" for name, col in funcs) or "rows"
        return f"{target} — {fn_text}"

    if step.op == "global_aggregate":
        return f"over {n_partitions_in_plan} partitions"

    if step.op == "gather":
        return f"collect rows from {n_partitions_in_plan} partitions"

    # Fallback: show whatever we have.
    if step.partition_id:
        return step.partition_id
    return ""
