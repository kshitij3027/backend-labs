"""Dashboard statistics aggregator.

One read-only function lives here: :func:`compute_dashboard_stats`. It
takes an async session and rolls up the four numbers + two breakdown
dicts + the recent-reports list that the dashboard's stats card and
recent-card render.

Why a separate service module (vs. inlining in the route)? Two reasons:

  1. The stats logic is non-trivial — five queries, one rate
     calculation, one ORM-to-Pydantic mapping. Pulling it out keeps
     the route handler readable and unit-testable in isolation.
  2. The same aggregator gets reused by the HTMX dashboard partial in
     commit 16, so we want one source of truth for "what does the
     dashboard see?".

``success_rate`` deliberately excludes in-flight reports (PENDING /
AGGREGATING / EXPORTING / SIGNING). A report still mid-flight isn't
a failure — counting it would make the rate look much worse than it
is. When no terminal reports exist yet the rate is reported as
``0.0`` so the dashboard can render a clean "n/a"-style placeholder
without divide-by-zero panic.
"""
from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..api.schemas import DashboardStats, RecentReportSummary
from ..persistence.models import Report


# Non-terminal states — used for the in-flight counter and to exclude
# these rows from the success_rate denominator. Kept in sync with the
# ReportState enum but as a plain string set so this module doesn't
# pull in the enum just for membership checks.
_IN_FLIGHT_STATES: tuple[str, ...] = (
    "PENDING",
    "AGGREGATING",
    "EXPORTING",
    "SIGNING",
)


async def compute_dashboard_stats(
    session: AsyncSession,
    *,
    recent_limit: int = 10,
) -> DashboardStats:
    """Roll up the dashboard counters from ``reports``.

    Args:
        session: Open async session.
        recent_limit: How many of the most recent reports to return in
            the ``recent`` list (default 10 — matches the partial in
            commit 16).

    Returns:
        A fully-populated :class:`DashboardStats`.
    """
    # --- Totals + state buckets ---
    total_reports = (
        await session.execute(select(func.count()).select_from(Report))
    ).scalar_one() or 0

    completed_count = (
        await session.execute(
            select(func.count())
            .select_from(Report)
            .where(Report.state == "COMPLETED")
        )
    ).scalar_one() or 0

    failed_count = (
        await session.execute(
            select(func.count())
            .select_from(Report)
            .where(Report.state == "FAILED")
        )
    ).scalar_one() or 0

    in_flight_count = (
        await session.execute(
            select(func.count())
            .select_from(Report)
            .where(Report.state.in_(_IN_FLIGHT_STATES))
        )
    ).scalar_one() or 0

    # --- Framework + format breakdowns ---
    # Group-by counts; cast both halves to plain ints so the response
    # body is JSON-clean (some drivers return Decimal-ish wrappers).
    fw_rows = (
        await session.execute(
            select(Report.framework, func.count(Report.id))
            .group_by(Report.framework)
            .order_by(Report.framework)
        )
    ).all()
    framework_breakdown = {fw: int(cnt) for fw, cnt in fw_rows}

    fmt_rows = (
        await session.execute(
            select(Report.export_format, func.count(Report.id))
            .group_by(Report.export_format)
            .order_by(Report.export_format)
        )
    ).all()
    format_breakdown = {fmt: int(cnt) for fmt, cnt in fmt_rows}

    # --- Success rate ---
    # COMPLETED / (COMPLETED + FAILED). In-flight rows are excluded —
    # they aren't failures, and lumping them into the denominator
    # would make the rate look artificially low whenever generation is
    # busy. 0.0 when no terminal rows exist.
    terminal_total = completed_count + failed_count
    success_rate = (
        round(completed_count / terminal_total, 4) if terminal_total > 0 else 0.0
    )

    # --- Recent reports ---
    recent_rows = (
        await session.execute(
            select(Report)
            .order_by(Report.created_at.desc())
            .limit(recent_limit)
        )
    ).scalars().all()
    recent = [
        RecentReportSummary(
            report_id=row.id,
            framework=row.framework,
            export_format=row.export_format,
            state=row.state,
            created_at=row.created_at,
            completed_at=row.completed_at,
        )
        for row in recent_rows
    ]

    return DashboardStats(
        total_reports=int(total_reports),
        framework_breakdown=framework_breakdown,
        format_breakdown=format_breakdown,
        success_rate=float(success_rate),
        in_flight=int(in_flight_count),
        recent=recent,
    )
