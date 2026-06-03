"""Query access-shape classification.

A single pure function, :func:`classify_query`, that maps a query's
projection (``columns``) and ``aggregations`` onto a :class:`~src.models.QueryClass`.
The result guides downstream format selection (row vs. columnar vs. hybrid):
analytical queries favour columnar layouts, full-record reads favour row
layouts, and mixed queries sit in between.

The function has no I/O and holds no state, so it is trivially testable and
safe to call on any hot path.
"""
from __future__ import annotations

from src.models import QueryClass

__all__ = ["classify_query"]


def classify_query(
    columns: list[str] | None,
    aggregations: list,
    *,
    analytical_max: int,
    full_record_min: int,
) -> QueryClass:
    """Classify a query's access shape, applying rules in strict order.

    The rules below are evaluated top-to-bottom and the **first match wins**.
    The ordering is deliberate: an aggregation is treated as analytical before
    column count is ever considered.

    Rules:
        1. Non-empty ``aggregations`` -> ``ANALYTICAL`` (an aggregation is
           analytical regardless of how many columns are projected).
        2. ``columns is None`` -> ``FULL_RECORD`` (no projection means the whole
           record is read back).
        3. ``len(columns) <= analytical_max`` -> ``ANALYTICAL``.
        4. ``len(columns) > full_record_min`` -> ``FULL_RECORD``.
        5. Otherwise -> ``MIXED``.

    Boundaries (with the default thresholds ``analytical_max=3`` /
    ``full_record_min=10``):
        * ``columns=None`` -> ``FULL_RECORD``
        * an empty list (``[]``, length 0) -> ``ANALYTICAL`` (``0 <= 3``)
        * 0..3 columns -> ``ANALYTICAL``
        * 4..10 columns -> ``MIXED``
        * 11+ columns -> ``FULL_RECORD``
        * any aggregation -> ``ANALYTICAL`` even alongside many columns

    Args:
        columns: The projected column names, or ``None`` for the whole record.
        aggregations: The query's aggregations (e.g. ``list[Aggregation]``);
            only emptiness is inspected.
        analytical_max: Inclusive upper bound on column count for a projection
            to count as analytical.
        full_record_min: Exclusive lower bound on column count above which a
            projection counts as a full-record read.

    Returns:
        The :class:`~src.models.QueryClass` describing the query's access shape.
    """
    # Rule 1: an aggregation is analytical irrespective of projection width.
    if aggregations:
        return QueryClass.ANALYTICAL

    # Rule 2: no projection means the caller wants the whole record.
    if columns is None:
        return QueryClass.FULL_RECORD

    n = len(columns)

    # Rule 3: a narrow projection (including the empty list) is analytical.
    if n <= analytical_max:
        return QueryClass.ANALYTICAL

    # Rule 4: a very wide projection is effectively a full-record read.
    if n > full_record_min:
        return QueryClass.FULL_RECORD

    # Rule 5: everything in between is mixed.
    return QueryClass.MIXED
