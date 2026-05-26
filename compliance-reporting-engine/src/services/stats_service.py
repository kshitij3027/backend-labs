"""Dashboard statistics aggregator.

The headline export here is :func:`compute_dashboard_stats`, which
rolls up the four numbers + two breakdown dicts + the recent-reports
list that the dashboard's stats card renders.

Three smaller helpers feed the commit-16 HTMX partial cards:

  * :func:`list_recent_reports` — last N report rows shaped into
    :class:`RecentCardRow` (adds the short id, formatted timestamp,
    and download URL the template expects).
  * :func:`list_in_flight_reports` — same shape, filtered to
    non-terminal states (PENDING / AGGREGATING / EXPORTING / SIGNING).
  * :func:`framework_breakdown` — ``(framework, count, percent_int)``
    tuples sorted by count DESC, ready for the stacked-bar partial.

Why a separate service module (vs. inlining in the route)? Two reasons:

  1. The stats logic is non-trivial — five queries, one rate
     calculation, one ORM-to-Pydantic mapping. Pulling it out keeps
     the route handler readable and unit-testable in isolation.
  2. The same aggregators get reused by the HTMX dashboard partials,
     so we want one source of truth for "what does the dashboard see?".

``success_rate`` deliberately excludes in-flight reports (PENDING /
AGGREGATING / EXPORTING / SIGNING). A report still mid-flight isn't
a failure — counting it would make the rate look much worse than it
is. When no terminal reports exist yet the rate is reported as
``0.0`` so the dashboard can render a clean "n/a"-style placeholder
without divide-by-zero panic.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict
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


# Format used by the recent / in-flight cards to render the
# ``created_at`` column. Year-month-day plus hour:minute keeps the
# table compact while still letting an operator eyeball "is this
# recent?" without expanding the column.
_TIMESTAMP_DISPLAY_FORMAT = "%Y-%m-%d %H:%M"


class RecentCardRow(BaseModel):
    """One row rendered by the recent / in-flight HTMX partial cards.

    A thin presentation-layer model: the fields here exist purely to
    feed the Jinja templates without forcing the template to call
    helper functions. ``report_id_short`` is the first 8 hex chars of
    the UUID (more than enough to disambiguate in a 10-row table) and
    ``download_url`` is only set on COMPLETED rows so the template can
    conditionally render the download link without an extra state
    check.
    """

    model_config = ConfigDict(from_attributes=True)

    report_id: UUID
    report_id_short: str
    framework: str
    export_format: str
    state: str
    created_at: datetime
    created_at_short: str
    completed_at: Optional[datetime] = None
    download_url: Optional[str] = None


def _shape_card_row(report: Report) -> RecentCardRow:
    """Project a ``Report`` ORM row onto a :class:`RecentCardRow`.

    Two derived fields land here so the template stays declarative:
    the 8-char short id and the human-friendly ``YYYY-MM-DD HH:MM``
    timestamp. ``download_url`` is only populated for COMPLETED rows
    — the download endpoint 404s otherwise, so surfacing the link on
    a half-done report would just lead a curious user to a dead end.
    """
    report_id_str = str(report.id)
    download_url: Optional[str] = (
        f"/reports/{report.id}/download" if report.state == "COMPLETED" else None
    )
    created_short = (
        report.created_at.strftime(_TIMESTAMP_DISPLAY_FORMAT)
        if report.created_at is not None
        else ""
    )
    return RecentCardRow(
        report_id=report.id,
        report_id_short=report_id_str[:8],
        framework=report.framework,
        export_format=report.export_format,
        state=report.state,
        created_at=report.created_at,
        created_at_short=created_short,
        completed_at=report.completed_at,
        download_url=download_url,
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


async def list_recent_reports(
    session: AsyncSession,
    *,
    limit: int = 10,
) -> list[RecentCardRow]:
    """Fetch the last ``limit`` reports, newest first, shaped for the recent card.

    Used by ``GET /partials/recent``. The shaping (``report_id_short``,
    ``created_at_short``, conditional ``download_url``) happens in
    :func:`_shape_card_row` so the template stays presentation-free.
    """
    rows = (
        await session.execute(
            select(Report)
            .order_by(Report.created_at.desc())
            .limit(limit)
        )
    ).scalars().all()
    return [_shape_card_row(row) for row in rows]


async def list_in_flight_reports(session: AsyncSession) -> list[RecentCardRow]:
    """Fetch every report still in a non-terminal state, newest first.

    "In-flight" means PENDING / AGGREGATING / EXPORTING / SIGNING — the
    same set that drives the in-flight counter on the stats card. The
    dashboard polls this endpoint on the same cadence, so each pass
    naturally shows reports moving along the pipeline.
    """
    rows = (
        await session.execute(
            select(Report)
            .where(Report.state.in_(_IN_FLIGHT_STATES))
            .order_by(Report.created_at.desc())
        )
    ).scalars().all()
    return [_shape_card_row(row) for row in rows]


async def list_finhealth_reports(
    session: AsyncSession,
    *,
    limit: int = 5,
) -> list[dict]:
    """Last N FinHealth reports + the count of composite_risk findings.

    The composite_risk count isn't stored on the row — it's a finding,
    which only lives in the rendered report payload. So we return the
    report row and let the template render whatever it can pull off the
    Report model (``signature_secondary_hex`` presence is a good proxy
    for "dual-signed and ready").

    Returned dicts are shaped for direct template consumption:
    ``report_id_short`` is the first 8 hex chars of the UUID,
    ``created_at_short`` is a YYYY-MM-DD HH:MM rendering, and
    ``signature_present`` / ``secondary_signature_present`` are
    pre-computed booleans so the Jinja partial stays declarative.
    """
    rows = (
        await session.execute(
            select(Report)
            .where(Report.framework == "FINHEALTH")
            .order_by(Report.created_at.desc())
            .limit(limit)
        )
    ).scalars().all()

    results: list[dict] = []
    for row in rows:
        report_id_str = str(row.id)
        created_short = (
            row.created_at.strftime(_TIMESTAMP_DISPLAY_FORMAT)
            if row.created_at is not None
            else ""
        )
        download_url: Optional[str] = (
            f"/reports/{row.id}/download" if row.state == "COMPLETED" else None
        )
        results.append(
            {
                "report_id": row.id,
                "report_id_short": report_id_str[:8],
                "framework": row.framework,
                "export_format": row.export_format,
                "state": row.state,
                "created_at": row.created_at,
                "created_at_short": created_short,
                "signature_present": bool(row.signature_hex),
                "secondary_signature_present": bool(row.signature_secondary_hex),
                "download_url": download_url,
            }
        )
    return results


async def framework_breakdown(
    session: AsyncSession,
) -> list[tuple[str, int, int]]:
    """Aggregate report counts per framework with integer percentages.

    Returns a list of ``(framework, count, percent_int)`` tuples sorted
    by count DESC, ties broken by framework name. ``percent_int`` is
    ``round(count / total * 100)`` so the stacked-bar segments add up
    to a clean 100% (within ±1 % rounding noise — acceptable for a
    visual breakdown).

    Returns an empty list when no reports exist, letting the template
    render an "empty" placeholder without divide-by-zero panic.
    """
    rows = (
        await session.execute(
            select(Report.framework, func.count(Report.id))
            .group_by(Report.framework)
        )
    ).all()
    if not rows:
        return []

    counts: list[tuple[str, int]] = [(fw, int(cnt)) for fw, cnt in rows]
    total = sum(cnt for _, cnt in counts)
    if total <= 0:
        return []

    # Sort by count DESC, framework ASC for stable ordering. The
    # template iterates the list in order, so the largest segment
    # lands on the left of the stacked bar.
    counts.sort(key=lambda pair: (-pair[1], pair[0]))

    return [(fw, cnt, round(cnt / total * 100)) for fw, cnt in counts]
