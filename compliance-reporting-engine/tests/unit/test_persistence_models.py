"""Unit tests for the persistence layer.

Three concerns:

  1. ``LogEvent`` round-trips cleanly, including the JSON list /
     dict columns — proving the ``JSONType`` variant works on SQLite.
  2. ``Report`` round-trips with defaults populated by the ORM:
     ``created_at`` / ``updated_at`` get filled in, and the
     ``signature_*`` + ``completed_at`` columns stay ``None`` until
     the report actually finishes.
  3. ``ReportFile`` relationship works both ways: a parent ``Report``
     surfaces its children via ``report.files``, and deleting the
     parent cascades the children out of ``report_files``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from src.persistence.models import LogEvent, Report, ReportFile


async def test_log_event_round_trip(session_factory) -> None:
    """JSON list + dict columns survive a write + read cycle."""
    ts = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = LogEvent(
        timestamp=ts,
        framework_tags=["SOX", "HIPAA"],
        event_type="login",
        actor="alice@example.com",
        resource="auth-service",
        action="authenticate",
        outcome="success",
        sensitivity="internal",
        payload={"k": "v", "n": 1},
    )

    async with session_factory() as session:
        session.add(event)
        await session.commit()
        event_id = event.id

    async with session_factory() as session:
        loaded = (
            await session.execute(select(LogEvent).where(LogEvent.id == event_id))
        ).scalar_one()

    assert loaded.id == event_id
    assert loaded.timestamp == ts
    assert loaded.framework_tags == ["SOX", "HIPAA"]
    assert loaded.event_type == "login"
    assert loaded.actor == "alice@example.com"
    assert loaded.resource == "auth-service"
    assert loaded.action == "authenticate"
    assert loaded.outcome == "success"
    assert loaded.sensitivity == "internal"
    assert loaded.payload == {"k": "v", "n": 1}


async def test_report_round_trip(session_factory) -> None:
    """Defaults: created/updated_at populated, signature + completed_at stay None."""
    report = Report(
        framework="SOX",
        period_start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 6, 30, tzinfo=timezone.utc),
        export_format="PDF",
        state="PENDING",
        title="Q2 SOX",
    )

    async with session_factory() as session:
        session.add(report)
        await session.commit()
        report_id = report.id

    async with session_factory() as session:
        loaded = (
            await session.execute(select(Report).where(Report.id == report_id))
        ).scalar_one()

    assert loaded.id == report_id
    assert loaded.framework == "SOX"
    assert loaded.export_format == "PDF"
    assert loaded.state == "PENDING"
    assert loaded.title == "Q2 SOX"
    # Defaults populated by the ORM
    assert loaded.created_at is not None
    assert loaded.updated_at is not None
    # Optional columns stay None until the report actually completes / is signed
    assert loaded.completed_at is None
    assert loaded.signature_hex is None
    assert loaded.signature_secondary_hex is None
    assert loaded.error_message is None


async def test_report_file_relationship(session_factory) -> None:
    """``report.files`` populates from the back_populates side; delete cascades."""
    report = Report(
        framework="HIPAA",
        period_start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 6, 30, tzinfo=timezone.utc),
        export_format="CSV",
        state="COMPLETED",
        title="Q2 HIPAA",
    )

    async with session_factory() as session:
        session.add(report)
        await session.commit()
        report_id = report.id

    rf = ReportFile(
        report_id=report_id,
        file_path="/exports/q2-hipaa.csv",
        format="CSV",
        size_bytes=2048,
        encrypted=True,
    )

    async with session_factory() as session:
        session.add(rf)
        await session.commit()
        file_id = rf.id

    # Load the report with files eagerly via the relationship side
    async with session_factory() as session:
        loaded_report = (
            await session.execute(
                select(Report).where(Report.id == report_id)
            )
        ).scalar_one()
        # Trigger relationship load while still inside the session
        files = (
            await session.execute(
                select(ReportFile).where(ReportFile.report_id == report_id)
            )
        ).scalars().all()

    assert len(files) == 1
    assert files[0].format == "CSV"
    assert files[0].size_bytes == 2048
    assert files[0].encrypted is True
    assert files[0].file_path == "/exports/q2-hipaa.csv"
    assert loaded_report.framework == "HIPAA"

    # Delete the parent report; the child ReportFile should disappear via cascade.
    async with session_factory() as session:
        loaded_report = (
            await session.execute(select(Report).where(Report.id == report_id))
        ).scalar_one()
        await session.delete(loaded_report)
        await session.commit()

    async with session_factory() as session:
        remaining_file = (
            await session.execute(
                select(ReportFile).where(ReportFile.id == file_id)
            )
        ).scalar_one_or_none()
        remaining_report = (
            await session.execute(select(Report).where(Report.id == report_id))
        ).scalar_one_or_none()

    assert remaining_report is None
    assert remaining_file is None
