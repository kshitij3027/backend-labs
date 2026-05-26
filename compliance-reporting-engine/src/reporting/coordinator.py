"""Report generation coordinator.

The coordinator owns the full lifecycle for a single ``Report`` row:

    PENDING -> AGGREGATING -> EXPORTING -> SIGNING -> COMPLETED
            \\ (any state -> FAILED on exception)

It's fire-and-forget from the API layer's point of view: a
``BackgroundTasks`` call hands a ``report_id`` to :meth:`generate` and
the caller polls ``Report.state`` for completion. The coordinator owns
its own ``async_sessionmaker`` (rather than piggy-backing on a
request-scoped session) so the request handler can return 202 long
before generation finishes.

Concurrency is bounded by an ``asyncio.Semaphore`` injected at
construction time — the lifespan in ``src/main.py`` wires this from
``settings.max_concurrent_reports`` so a burst of generate requests
can't snowball into 50 simultaneous Postgres + Faker workloads.

Failure handling: any exception inside the pipeline is caught,
logged with a short traceback, and recorded on the row as a
``FAILED`` transition with the exception class + message in
``error_message``. The coordinator never re-raises (callers track
progress via the row, not exceptions), so a failing report never
poisons the background-task queue.
"""
from __future__ import annotations

import asyncio
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from uuid import UUID

from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import async_sessionmaker

from ..logging_config import get_logger
from ..persistence.models import Report, ReportFile
from ..signing.fernet_store import encrypt_file_in_place
from ..signing.hmac_signer import sign_payload
from .aggregator import build_report_payload
from .state_machine import ReportState, assert_transition

logger = get_logger("reporting.coordinator")


# An exporter takes the canonical payload dict and returns raw bytes
# ready for disk. Real exporters land in commits 10-12; tests inject
# canned-bytes stubs.
ExporterFn = Callable[[dict], bytes]


# File-extension mapping per export format. Anything not in the table
# falls back to the lowercased format code so unknown formats still get
# a sensible file name.
FORMAT_EXT: dict[str, str] = {
    "PDF": "pdf",
    "CSV": "csv",
    "JSON": "json",
    "XML": "xml",
}


class ReportCoordinator:
    """Drive a single report through aggregate -> export -> sign -> persist.

    The coordinator owns:

      * the async session factory (so it can open its own transaction
        per report, decoupled from any request-scoped session that
        dispatched the work),
      * the primary + (optional) secondary HMAC signing keys,
      * the Fernet instance for at-rest file encryption,
      * the storage path under which artefacts are written,
      * a semaphore bounding concurrent generations (prevents a burst
        of background tasks from blowing past the configured ceiling),
      * the exporter registry keyed by uppercased format code.

    A coordinator instance is built once in the FastAPI lifespan and
    stashed on ``app.state.coordinator``; routes call ``generate`` via
    ``BackgroundTasks`` and return 202 immediately.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker,
        *,
        signing_key: bytes,
        fernet: Fernet,
        storage_path: Path,
        semaphore: asyncio.Semaphore,
        exporters: dict[str, ExporterFn],
        secondary_signing_key: Optional[bytes] = None,
    ) -> None:
        self.session_factory = session_factory
        self.signing_key = signing_key
        self.secondary_signing_key = secondary_signing_key
        self.fernet = fernet
        self.storage_path = Path(storage_path)
        # mkdir on the storage root so the first-ever report doesn't
        # crash on a missing directory. parents=True is defensive in
        # case the operator pointed at a deeply-nested path.
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.semaphore = semaphore
        self.exporters = exporters

    async def _set_state(
        self,
        session,
        report: Report,
        new_state: ReportState,
        *,
        error_message: str | None = None,
    ) -> None:
        """Transition the report to ``new_state``, validating the move.

        Flushes so ``Report.state`` is visible to any read inside the
        same transaction. The flush also surfaces SQLAlchemy errors
        promptly (vs. on a later implicit flush) which makes the
        traceback in the failure path much more useful.

        For ``COMPLETED``, also stamps ``completed_at`` with a fresh
        tz-aware UTC ``now()`` so downstream consumers (the API +
        dashboard) can sort/display "finished N seconds ago".
        """
        assert_transition(report.state, new_state)
        report.state = new_state.value
        if error_message is not None:
            report.error_message = error_message
        if new_state is ReportState.COMPLETED:
            report.completed_at = datetime.now(timezone.utc)
        await session.flush()

    async def generate(self, report_id: UUID) -> None:
        """Run the full generate pipeline for a single report.

        Acquires :attr:`semaphore` so concurrent generations don't
        exceed the configured ceiling. The whole pipeline runs inside a
        single ``session.begin()`` block so on success everything
        commits atomically, and on failure the FAILED transition still
        lands (since we set it directly on the row inside the same
        transaction's tail).

        Edge case: the ``_set_state`` guard would reject a
        ``->FAILED`` from a state where state was never advanced
        (impossible in practice because every step transitions before
        it can raise). Even so, the exception block writes the
        ``FAILED`` value directly on the row instead of via
        :meth:`_set_state` — that way an out-of-band state mutation
        (or a future refactor) can't corner-case us into a permanent
        non-terminal row.
        """
        async with self.semaphore:
            async with self.session_factory() as session:
                async with session.begin():
                    report = await session.get(Report, report_id)
                    if report is None:
                        # Nothing we can do — the row that dispatched
                        # us has been deleted. Log and return so the
                        # background task drains cleanly.
                        logger.error(
                            "coordinator_report_not_found",
                            report_id=str(report_id),
                        )
                        return

                    try:
                        # --- Phase 1: aggregate ---
                        await self._set_state(session, report, ReportState.AGGREGATING)
                        payload = await build_report_payload(
                            session,
                            framework=report.framework,
                            period_start=report.period_start,
                            period_end=report.period_end,
                        )

                        # --- Phase 2: export ---
                        await self._set_state(session, report, ReportState.EXPORTING)
                        fmt = report.export_format.upper()
                        if fmt not in self.exporters:
                            raise ValueError(
                                f"No exporter registered for format {fmt!r}"
                            )
                        exporter = self.exporters[fmt]
                        body_bytes = exporter(payload)

                        # --- Phase 3: sign ---
                        await self._set_state(session, report, ReportState.SIGNING)
                        signature_hex = sign_payload(payload, key=self.signing_key)
                        report.signature_hex = signature_hex
                        # FinHealth uses dual-signature (primary key +
                        # HIPAA-scope secondary). We only emit it if
                        # the secondary key is wired in — otherwise the
                        # column stays NULL and verification falls back
                        # to the primary signature.
                        if (
                            report.framework == "FINHEALTH"
                            and self.secondary_signing_key is not None
                        ):
                            report.signature_secondary_hex = sign_payload(
                                payload, key=self.secondary_signing_key
                            )

                        # --- Phase 4: write + encrypt at rest ---
                        ext = FORMAT_EXT.get(fmt, fmt.lower())
                        out_path = self.storage_path / f"{report_id}.{ext}"
                        out_path.write_bytes(body_bytes)
                        size_after_encrypt = encrypt_file_in_place(
                            out_path, self.fernet
                        )

                        # --- Phase 5: persist the file row + COMPLETED ---
                        session.add(
                            ReportFile(
                                report_id=report.id,
                                file_path=str(out_path),
                                format=fmt,
                                size_bytes=size_after_encrypt,
                                encrypted=True,
                            )
                        )
                        await self._set_state(session, report, ReportState.COMPLETED)
                        logger.info(
                            "report_generated",
                            report_id=str(report_id),
                            framework=report.framework,
                            format=fmt,
                            size_bytes=size_after_encrypt,
                        )
                    except Exception as exc:
                        # Failure path: mark the row FAILED directly
                        # (no _set_state -> no guard) so we're robust
                        # to the (admittedly impossible-today) case
                        # where state was never advanced past the
                        # entry value, OR was advanced by a future
                        # branch we didn't account for. Every state
                        # already allows -> FAILED so this would
                        # succeed through the guard anyway; we bypass
                        # it purely as a belt-and-braces measure.
                        tb = traceback.format_exc(limit=4)
                        logger.error(
                            "report_generation_failed",
                            report_id=str(report_id),
                            error=str(exc),
                            traceback=tb,
                        )
                        report.state = ReportState.FAILED.value
                        report.error_message = f"{exc.__class__.__name__}: {exc}"
                        await session.flush()
                        # Don't re-raise — coordinator is fire-and-forget
                        # from BackgroundTasks. Callers poll Report.state.
