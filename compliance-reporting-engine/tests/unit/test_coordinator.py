"""Unit tests for :mod:`src.reporting.coordinator`.

Two end-to-end scenarios run against the in-memory SQLite engine
fixture:

  * Happy path — seed SOX-tagged events, dispatch ``generate`` with a
    canned-bytes JSON exporter stub, and assert the Report row lands
    on COMPLETED, the signature is populated, the ReportFile row is
    written, and the on-disk file has been Fernet-encrypted (i.e. its
    contents differ from the canned plaintext the stub returned).
  * Failure path — the exporter raises ``RuntimeError("boom")``;
    the coordinator must catch it, record FAILED + the error message
    on the row, and not re-raise (so background-task queues don't
    poison themselves on a single bad report).

Real exporters land in commits 10-12; this test injects a tiny stub
that returns canned bytes so the coordinator's plumbing is exercised
independently.
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from cryptography.fernet import Fernet
from sqlalchemy import select

from src.logs.repository import insert_log_events
from src.logs.seeder import generate_synthetic_logs
from src.persistence.models import Report, ReportFile
from src.reporting.coordinator import ReportCoordinator


# Hex digest regex: HMAC-SHA256 = 64 hex chars.
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")


def _make_coordinator(
    session_factory,
    *,
    storage_path: Path,
    exporters: dict,
) -> ReportCoordinator:
    """Build a ReportCoordinator with deterministic test inputs.

    Uses a fixed 32-byte signing key + a fresh Fernet key + a 2-slot
    semaphore. The storage path is the per-test ``tmp_path`` so
    artefacts don't leak between tests.
    """
    return ReportCoordinator(
        session_factory=session_factory,
        signing_key=b"a" * 32,
        fernet=Fernet(Fernet.generate_key()),
        storage_path=storage_path,
        semaphore=asyncio.Semaphore(2),
        exporters=exporters,
    )


async def _seed_sox_window(session_factory) -> tuple[datetime, datetime]:
    """Seed ~20 SOX-tagged events over a 7-day window and return the bounds."""
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)
    events = generate_synthetic_logs(
        20,
        frameworks=["SOX"],
        seed=123,
        period_start=period_start,
        period_end=period_end,
    )
    async with session_factory() as session:
        await insert_log_events(session, events)
        await session.commit()
    return period_start, period_end


async def _insert_pending_report(
    session_factory,
    *,
    framework: str,
    period_start: datetime,
    period_end: datetime,
    export_format: str = "JSON",
) -> UUID:
    """Insert a PENDING Report row and return its id."""
    async with session_factory() as session:
        report = Report(
            framework=framework,
            period_start=period_start,
            period_end=period_end,
            export_format=export_format,
            state="PENDING",
            title="Test Report",
            description="Coordinator unit test",
        )
        session.add(report)
        await session.commit()
        return report.id


async def test_coordinator_happy_path(session_factory, tmp_path: Path) -> None:
    """End-to-end COMPLETED transition: signature populated, file encrypted at rest."""
    period_start, period_end = await _seed_sox_window(session_factory)
    report_id = await _insert_pending_report(
        session_factory,
        framework="SOX",
        period_start=period_start,
        period_end=period_end,
        export_format="JSON",
    )

    # Canned-bytes exporter stub — keeps the coordinator independent of
    # the real exporters (which land in commits 10-12).
    stub_bytes = b"STUB BYTES FROM EXPORTER\n"
    coordinator = _make_coordinator(
        session_factory,
        storage_path=tmp_path,
        exporters={"JSON": lambda payload: stub_bytes},
    )

    await coordinator.generate(report_id)

    # --- Verify Report row transitioned to COMPLETED ---
    async with session_factory() as session:
        report = await session.get(Report, report_id)
        assert report is not None
        assert report.state == "COMPLETED"
        # Signature is a 64-char lowercase hex string (HMAC-SHA256).
        assert report.signature_hex is not None
        assert _HEX_64_RE.match(report.signature_hex)
        # FinHealth secondary signature only fires for FinHealth + a
        # secondary key — neither is set here.
        assert report.signature_secondary_hex is None
        assert report.completed_at is not None
        assert report.error_message is None

        # --- Verify ReportFile row exists + points at the expected path ---
        result = await session.execute(
            select(ReportFile).where(ReportFile.report_id == report_id)
        )
        files = result.scalars().all()
        assert len(files) == 1
        file_row = files[0]
        assert file_row.format == "JSON"
        assert file_row.encrypted is True
        assert file_row.size_bytes > 0
        # The path the coordinator should have used.
        expected_path = tmp_path / f"{report_id}.json"
        assert Path(file_row.file_path) == expected_path

    # --- Verify the file exists on disk and is Fernet-encrypted ---
    # (i.e. its bytes are NOT the canned stub_bytes any more.)
    assert expected_path.exists()
    on_disk = expected_path.read_bytes()
    assert on_disk != stub_bytes
    # Fernet ciphertext starts with the version byte 0x80 (base64-encoded -> "gAAAAA").
    assert on_disk.startswith(b"gAAAAA")
    # File size on disk should match what we persisted.
    assert len(on_disk) == file_row.size_bytes


async def test_coordinator_failure_path(session_factory, tmp_path: Path) -> None:
    """Exporter raises -> Report.state = FAILED, error_message captures the exception."""
    period_start, period_end = await _seed_sox_window(session_factory)
    report_id = await _insert_pending_report(
        session_factory,
        framework="SOX",
        period_start=period_start,
        period_end=period_end,
        export_format="JSON",
    )

    # Exporter that raises — using a generator expression so the
    # function body of the lambda is evaluated lazily on call.
    def _exploding_exporter(payload):  # noqa: ARG001
        raise RuntimeError("boom")

    coordinator = _make_coordinator(
        session_factory,
        storage_path=tmp_path,
        exporters={"JSON": _exploding_exporter},
    )

    # Must NOT raise — coordinator catches everything.
    await coordinator.generate(report_id)

    async with session_factory() as session:
        report = await session.get(Report, report_id)
        assert report is not None
        assert report.state == "FAILED"
        assert report.error_message is not None
        # The error_message should carry both the class name and the
        # message text so debugging is one log line away.
        assert "RuntimeError" in report.error_message
        assert "boom" in report.error_message
        # No signature recorded on a failed report.
        assert report.signature_hex is None
        # And we didn't stamp completed_at — that's reserved for COMPLETED.
        assert report.completed_at is None

        # No ReportFile row was written.
        result = await session.execute(
            select(ReportFile).where(ReportFile.report_id == report_id)
        )
        files = result.scalars().all()
        assert files == []


async def test_coordinator_missing_format_marks_failed(
    session_factory, tmp_path: Path
) -> None:
    """Asking for a format that has no exporter registered ends in FAILED, not a crash."""
    period_start, period_end = await _seed_sox_window(session_factory)
    report_id = await _insert_pending_report(
        session_factory,
        framework="SOX",
        period_start=period_start,
        period_end=period_end,
        export_format="PDF",  # PDF exporter intentionally absent
    )

    coordinator = _make_coordinator(
        session_factory,
        storage_path=tmp_path,
        exporters={"JSON": lambda payload: b""},  # PDF NOT registered
    )

    await coordinator.generate(report_id)

    async with session_factory() as session:
        report = await session.get(Report, report_id)
        assert report is not None
        assert report.state == "FAILED"
        assert "PDF" in (report.error_message or "")


async def test_coordinator_missing_report_id_is_a_noop(
    session_factory, tmp_path: Path
) -> None:
    """``generate`` for a non-existent report_id logs and returns without raising."""
    coordinator = _make_coordinator(
        session_factory,
        storage_path=tmp_path,
        exporters={"JSON": lambda payload: b"x"},
    )
    # Must NOT raise — the coordinator logs and returns early.
    await coordinator.generate(uuid4())


async def test_coordinator_creates_storage_dir(
    session_factory, tmp_path: Path
) -> None:
    """The storage path is mkdir'd at construction so the first ever report works."""
    deep_path = tmp_path / "a" / "b" / "c"
    assert not deep_path.exists()
    _make_coordinator(
        session_factory,
        storage_path=deep_path,
        exporters={"JSON": lambda payload: b"x"},
    )
    assert deep_path.exists()
    assert deep_path.is_dir()
