"""Local pre-reducer that runs on the mapper side to reduce network/storage overhead."""

from collections import defaultdict

import structlog

logger = structlog.get_logger()


def combine(kv_pairs: list[tuple], reduce_fn_name: str) -> list[tuple]:
    """Group mapper output by key and pre-aggregate using the reduce function.

    For example, [("error_404", 1), ("error_404", 1), ("error_500", 1)]
    becomes [("error_404", "2"), ("error_500", "1")] with sum reducer.
    """
    from src.mapfunctions.registry import get_reduce_fn

    reduce_fn = get_reduce_fn(reduce_fn_name)

    # Group by key
    groups = defaultdict(list)
    for key, value in kv_pairs:
        groups[key].append(value)

    # Apply reduce function to each group
    combined = []
    for key, values in groups.items():
        result = reduce_fn(values)
        combined.append((key, result))

    original_count = len(kv_pairs)
    combined_count = len(combined)
    if original_count > 0:
        ratio = 1 - (combined_count / original_count)
        logger.info(
            "combiner_applied",
            original_pairs=original_count,
            combined_pairs=combined_count,
            reduction_ratio=f"{ratio:.2%}",
        )

    return combined
