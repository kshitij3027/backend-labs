"""Built-in reduce functions."""

import json

from src.mapfunctions.registry import register_reduce


@register_reduce("sum")
def sum_reduce(values: list) -> str:
    """Sum all numeric values."""
    total = sum(float(v) for v in values)
    # Return as int string if the total is a whole number
    if total == int(total):
        return str(int(total))
    return str(total)


@register_reduce("count")
def count_reduce(values: list) -> str:
    """Count the number of values."""
    return str(len(values))


@register_reduce("collect")
def collect_reduce(values: list) -> str:
    """Collect distinct values as a JSON list."""
    return json.dumps(sorted(set(str(v) for v in values)))
