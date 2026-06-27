"""Metric ingestion: validate incoming points and bulk-insert them.

This is the single write path for historical metric observations. It accepts the
already-parsed :class:`~src.schemas.MetricIngest` points (Pydantic has enforced
finite values + non-empty names at the schema boundary), normalises timestamps
(default missing to ``now`` UTC, coerce naive to UTC), and persists them via
:func:`src.db.repository.add_metrics_bulk`.

Transaction handling
--------------------
``ingest_metrics`` commits its own transaction — ingestion is a self-contained
request-scoped unit of work, so the caller does not need to commit. On any error
the session is rolled back and the exception re-raised. Invalid input raises
:class:`ValueError`, which the API maps to HTTP 422.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy.orm import Session

from src.db import repository
from src.schemas import MetricIngest


def _normalise_timestamp(ts: datetime | None) -> datetime:
    """Default a missing timestamp to ``now`` UTC; coerce naive to UTC."""
    if ts is None:
        return datetime.now(timezone.utc)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def ingest_metrics(session: Session, points: Sequence[MetricIngest]) -> int:
    """Validate and bulk-insert ``points``; return the number of rows written.

    Args:
        session: An open SQLAlchemy session. This function commits it on success
            and rolls it back on failure.
        points: The metric points to ingest. Must be non-empty.

    Returns:
        The count of rows inserted.

    Raises:
        ValueError: If ``points`` is empty, or a point has a blank name or a
            non-finite value (defence in depth — schema validation should have
            caught these already).
    """
    if not points:
        raise ValueError("no metric points supplied")

    rows: list[dict] = []
    for p in points:
        name = (p.metric_name or "").strip()
        if not name:
            raise ValueError("metric_name must not be empty")
        if not math.isfinite(p.value):
            raise ValueError(f"value for {name!r} must be finite (no NaN/inf)")
        rows.append(
            {
                "metric_name": name,
                "timestamp": _normalise_timestamp(p.timestamp),
                "value": float(p.value),
            }
        )

    try:
        repository.add_metrics_bulk(session, rows, commit=True)
    except Exception:
        session.rollback()
        raise
    return len(rows)
