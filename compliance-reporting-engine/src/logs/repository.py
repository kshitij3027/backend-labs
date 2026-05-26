"""Async DAO for ``LogEvent`` reads and bulk writes.

Two operations live here:

  * :func:`insert_log_events` — bulk-inserts a batch of seeder-shaped
    dicts via ``session.add_all`` and flushes. The caller commits.
  * :func:`query_logs_for_framework_in_window` — pulls every event whose
    ``timestamp`` lies within ``[period_start, period_end]`` and whose
    ``framework_tags`` list contains the requested framework code.

JSON-contains is dialect-dependent: Postgres has ``JSONB @> '[..]'``,
SQLite needs ``json_each`` gymnastics. To stay dialect-agnostic at this
project's scale (~5000 events, occasional report build), we pull the
time-window slice with a portable ``BETWEEN`` predicate and then filter
the ``framework_tags`` membership in Python. This trades a small bit of
memory for one code path that runs unchanged on SQLite (tests) and
Postgres (prod) — see ADR-style comment inline below.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..logging_config import get_logger
from ..persistence.models import LogEvent

logger = get_logger("logs.repository")


async def insert_log_events(
    session: AsyncSession,
    events: list[dict[str, Any]],
) -> int:
    """Bulk-insert seeder-shaped dicts into ``log_events``.

    Args:
        session: An open async session. Caller is responsible for
            ``await session.commit()``.
        events: A list of dicts produced by
            :func:`src.logs.seeder.generate_synthetic_logs` (or any
            equivalent dict shaped like ``LogEvent`` columns).

    Returns:
        The number of rows added to the session.
    """
    if not events:
        return 0
    rows = [LogEvent(**event) for event in events]
    session.add_all(rows)
    await session.flush()
    logger.info("log_events_inserted", count=len(rows))
    return len(rows)


async def query_logs_for_framework_in_window(
    session: AsyncSession,
    framework: str,
    period_start: datetime,
    period_end: datetime,
) -> list[LogEvent]:
    """Return all ``LogEvent`` rows for ``framework`` in the time window.

    Trade-off: JSON-contains semantics differ across SQLite and Postgres,
    so this fetches every row in ``[period_start, period_end]`` with a
    portable ``BETWEEN`` predicate and filters ``framework_tags``
    membership in Python. Fine at this project's scale; if the dataset
    grows beyond ~1M events, swap in ``JSONB @> '["<framework>"]'`` on
    Postgres for an index-friendly predicate push-down.

    Args:
        session: An open async session.
        framework: Framework code (e.g. ``"SOX"``).
        period_start: Inclusive window start (tz-aware UTC).
        period_end: Inclusive window end (tz-aware UTC).

    Returns:
        Rows ordered by ``timestamp`` ascending.
    """
    stmt = (
        select(LogEvent)
        .where(LogEvent.timestamp.between(period_start, period_end))
        .order_by(LogEvent.timestamp.asc())
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    # FinHealth is a composite framework — the seeder doesn't tag events
    # with ``"FINHEALTH"`` directly. Instead, it matches any event tagged
    # with either of its two source frameworks (SOX or HIPAA).
    if framework == "FINHEALTH":
        composite_sources = ("SOX", "HIPAA")
        matches = [
            event
            for event in rows
            if event.framework_tags
            and any(tag in composite_sources for tag in event.framework_tags)
        ]
    else:
        matches = [
            event
            for event in rows
            if event.framework_tags and framework in event.framework_tags
        ]
    logger.info(
        "log_events_queried",
        framework=framework,
        window_count=len(rows),
        match_count=len(matches),
    )
    return matches
