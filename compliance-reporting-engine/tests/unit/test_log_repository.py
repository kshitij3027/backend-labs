"""Unit tests for :mod:`src.logs.repository`.

The repository module is tiny but it sits between the seeder and the
aggregator, so its contract matters:

  * Bulk insert returns the row count it actually added.
  * Framework membership filtering picks up SOX-only **and**
    SOX+HIPAA events when the caller asks for ``"SOX"`` — the
    membership semantics are "framework in tags" not "tags == [..]".
  * The time-window predicate is inclusive of bounds and excludes
    events outside the window even when their framework tags match.
  * Empty DB returns ``[]`` (not ``None``, not an error).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.logs.repository import (
    insert_log_events,
    query_logs_for_framework_in_window,
)


def _event(
    *,
    timestamp: datetime,
    framework_tags: list[str],
    event_type: str = "admin_login",
) -> dict:
    """Build a minimum-viable LogEvent-shaped dict for tests."""
    return {
        "timestamp": timestamp,
        "framework_tags": framework_tags,
        "event_type": event_type,
        "actor": "actor@example.com",
        "resource": "/test/resource",
        "action": "execute",
        "outcome": "success",
        "sensitivity": "internal",
        "payload": {"k": "v"},
    }


async def test_insert_and_query_by_framework(session_factory) -> None:
    """SOX-tagged events come back when queried for SOX, including dual-tagged ones."""
    base_ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    events: list[dict] = []
    # 10 SOX-only
    events.extend(
        _event(timestamp=base_ts + timedelta(minutes=i), framework_tags=["SOX"])
        for i in range(10)
    )
    # 10 HIPAA-only
    events.extend(
        _event(timestamp=base_ts + timedelta(minutes=10 + i), framework_tags=["HIPAA"])
        for i in range(10)
    )
    # 10 SOX + HIPAA
    events.extend(
        _event(
            timestamp=base_ts + timedelta(minutes=20 + i),
            framework_tags=["SOX", "HIPAA"],
        )
        for i in range(10)
    )

    async with session_factory() as session:
        inserted = await insert_log_events(session, events)
        await session.commit()
    assert inserted == 30

    window_start = base_ts - timedelta(minutes=1)
    window_end = base_ts + timedelta(hours=1)

    async with session_factory() as session:
        sox_rows = await query_logs_for_framework_in_window(
            session, "SOX", window_start, window_end
        )
        hipaa_rows = await query_logs_for_framework_in_window(
            session, "HIPAA", window_start, window_end
        )
        gdpr_rows = await query_logs_for_framework_in_window(
            session, "GDPR", window_start, window_end
        )

    assert len(sox_rows) == 20  # SOX-only + dual-tagged
    assert len(hipaa_rows) == 20  # HIPAA-only + dual-tagged
    assert len(gdpr_rows) == 0  # no GDPR-tagged events were inserted

    # Sorted ascending by timestamp.
    timestamps = [row.timestamp for row in sox_rows]
    assert timestamps == sorted(timestamps)


async def test_window_filter(session_factory) -> None:
    """Events outside the window are excluded even if framework tags match."""
    sox_window_center = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    inside_ts = [
        sox_window_center - timedelta(minutes=30),
        sox_window_center,
        sox_window_center + timedelta(minutes=30),
    ]
    outside_ts = [
        sox_window_center - timedelta(days=10),  # too early
        sox_window_center + timedelta(days=10),  # too late
    ]

    events = [_event(timestamp=ts, framework_tags=["SOX"]) for ts in inside_ts + outside_ts]

    async with session_factory() as session:
        await insert_log_events(session, events)
        await session.commit()

    window_start = sox_window_center - timedelta(hours=1)
    window_end = sox_window_center + timedelta(hours=1)

    async with session_factory() as session:
        rows = await query_logs_for_framework_in_window(
            session, "SOX", window_start, window_end
        )

    assert len(rows) == 3
    for row in rows:
        assert window_start <= row.timestamp <= window_end


async def test_empty_returns_empty(session_factory) -> None:
    """Querying an empty table returns ``[]`` rather than ``None`` or an error."""
    window_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    window_end = datetime(2026, 12, 31, tzinfo=timezone.utc)

    async with session_factory() as session:
        rows = await query_logs_for_framework_in_window(
            session, "SOX", window_start, window_end
        )

    assert rows == []
