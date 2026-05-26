"""Reports REST API — generate, status, download, verify.

Four endpoints make up the report lifecycle from the API's point of view:

  * ``POST /reports/generate`` — insert a PENDING row, dispatch the
    coordinator via ``BackgroundTasks``, return 202 + ``report_id``.
  * ``GET /reports/{id}`` — full status row (state machine + signature
    fields + download / verify URLs).
  * ``GET /reports/{id}/download`` — pull the encrypted artefact off
    disk, decrypt in-memory, return it as a streamed download with
    a sensible ``Content-Disposition`` filename.
  * ``GET /reports/{id}/verify`` — re-aggregate the payload, recompute
    the signature, constant-time-compare with the stored value, and
    return ``verified=true|false``. For FinHealth (dual-signed) the
    secondary signature is also verified against the secondary key.

The coordinator owns the actual generation pipeline; this router is
the thin HTTP surface that drops work into it and reads results back
out. Validation of ``framework`` against ``FRAMEWORK_REGISTRY`` lives
here (not in the Pydantic schema) so the schema module doesn't have
to import the framework registry — which would force a circular at
package-load time.
"""
from __future__ import annotations

from pathlib import Path
from uuid import UUID

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    HTTPException,
    Request,
    Response,
    status,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..frameworks import FRAMEWORK_REGISTRY
from ..persistence.models import Report, ReportFile
from ..reporting.aggregator import build_report_payload
from ..signing.fernet_store import decrypt_file_bytes
from ..signing.hmac_signer import verify_payload
from .dependencies import (
    get_secondary_signing_key,
    get_session,
    get_signing_key,
)
from .schemas import (
    GenerateReportRequest,
    GenerateReportResponse,
    ReportStatusResponse,
    VerifyResponse,
)


router = APIRouter(prefix="/reports", tags=["reports"])


# Map an upper-cased export format -> the MIME type the download
# endpoint advertises. Anything not in the table falls through to a
# generic octet-stream — the FORMAT_EXT table on the coordinator
# matches this set 1:1.
_FORMAT_MEDIA_TYPE: dict[str, str] = {
    "PDF": "application/pdf",
    "CSV": "text/csv",
    "JSON": "application/json",
    "XML": "application/xml",
}

# Same idea, but for the file extension on the downloaded filename.
# Matches the coordinator's FORMAT_EXT mapping.
_FORMAT_EXT: dict[str, str] = {
    "PDF": "pdf",
    "CSV": "csv",
    "JSON": "json",
    "XML": "xml",
}


def _status_response(report: Report) -> ReportStatusResponse:
    """Shape a ``Report`` row into the public status response.

    ``download_url`` is only populated for COMPLETED reports —
    surfacing it on a PENDING row would let a caller race the
    coordinator (and the download handler would 404 anyway).
    """
    download_url = (
        f"/reports/{report.id}/download" if report.state == "COMPLETED" else None
    )
    return ReportStatusResponse(
        report_id=report.id,
        state=report.state,
        framework=report.framework,
        export_format=report.export_format,
        period_start=report.period_start,
        period_end=report.period_end,
        created_at=report.created_at,
        completed_at=report.completed_at,
        error_message=report.error_message,
        download_url=download_url,
        verify_url=f"/reports/{report.id}/verify",
        signature_hex=report.signature_hex,
        signature_secondary_hex=report.signature_secondary_hex,
    )


@router.post(
    "/generate",
    response_model=GenerateReportResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate_report(
    payload: GenerateReportRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> GenerateReportResponse:
    """Insert a PENDING Report row and dispatch the coordinator.

    The route returns 202 with the row's id; the coordinator runs
    fire-and-forget against its own session (not this request's) so the
    response goes out the door before generation finishes.

    Validation runs here instead of inside the Pydantic schema so the
    schema module stays free of a ``frameworks`` import (which would
    create an import-time cycle). On any validation failure we raise
    422 with a ``detail`` matching FastAPI's standard shape so callers
    can surface a useful error message.
    """
    if payload.framework not in FRAMEWORK_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Unknown framework {payload.framework!r}; expected one of "
                f"{sorted(FRAMEWORK_REGISTRY.keys())}"
            ),
        )
    if payload.period_start >= payload.period_end:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="period_start must be < period_end",
        )

    report = Report(
        framework=payload.framework,
        period_start=payload.period_start,
        period_end=payload.period_end,
        export_format=payload.export_format,
        state="PENDING",
        title=payload.title,
        description=payload.description,
    )
    session.add(report)
    await session.flush()  # populate report.id while the row is still in scope
    report_id = report.id
    await session.commit()

    # Dispatch the coordinator after commit so the row is visible
    # when the background task opens its own session.
    coordinator = request.app.state.coordinator
    background_tasks.add_task(coordinator.generate, report_id)

    return GenerateReportResponse(report_id=report_id, state="PENDING")


@router.get("/{report_id}", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> ReportStatusResponse:
    """Return the full status row for a report or 404 if unknown."""
    report = await session.get(Report, report_id)
    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"report not found: {report_id}",
        )
    return _status_response(report)


@router.get("/{report_id}/download")
async def download_report(
    report_id: UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> Response:
    """Stream the decrypted artefact for a COMPLETED report.

    The on-disk bytes are Fernet-encrypted at rest; this handler
    decrypts in-memory and returns the plaintext with a sensible
    ``Content-Disposition`` so the caller's browser downloads it
    instead of trying to render the format inline.

    Edge cases:
      * Report row missing -> 404
      * Report not COMPLETED -> 404 (don't leak an "in progress"
        state via 409; the dashboard polls status separately)
      * No ReportFile row associated -> 404
      * File missing on disk -> 404 (decrypt would raise; surface as
        a clean not-found rather than a 500)
    """
    report = await session.get(Report, report_id)
    if report is None or report.state != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"report not available for download: {report_id}",
        )

    # Query the related ReportFile row explicitly. There's at most one
    # file per report in this commit; if multi-artefact bundles land
    # later we'll need a format selector on the URL. We avoid the
    # ``report.files`` lazy-load (async SQLAlchemy raises MissingGreenlet
    # on lazy attributes) by going straight to a plain ``select``.
    file_row = (
        await session.execute(
            select(ReportFile).where(ReportFile.report_id == report_id).limit(1)
        )
    ).scalar_one_or_none()
    if file_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no file artefact for report: {report_id}",
        )

    on_disk = Path(file_row.file_path)
    if not on_disk.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"file missing on disk for report: {report_id}",
        )

    fernet = request.app.state.fernet
    plaintext = decrypt_file_bytes(on_disk, fernet)

    fmt = (report.export_format or "").upper()
    media_type = _FORMAT_MEDIA_TYPE.get(fmt, "application/octet-stream")
    ext = _FORMAT_EXT.get(fmt, fmt.lower() or "bin")
    headers = {
        "Content-Disposition": f'attachment; filename="report-{report_id}.{ext}"'
    }
    return Response(content=plaintext, media_type=media_type, headers=headers)


@router.get("/{report_id}/verify", response_model=VerifyResponse)
async def verify_report(
    report_id: UUID,
    session: AsyncSession = Depends(get_session),
    signing_key: bytes = Depends(get_signing_key),
    secondary_signing_key: bytes | None = Depends(get_secondary_signing_key),
) -> VerifyResponse:
    """Re-aggregate the payload and verify the stored signature.

    Re-aggregating instead of caching the signed bytes is a deliberate
    trade-off: a tiny CPU hit on /verify in exchange for not having to
    keep the raw payload in storage. The signature was computed over
    the exact same dict shape the aggregator returns now, so as long
    as the underlying log events haven't changed the digest matches.

    For FinHealth (dual-signed) reports the secondary signature is
    also checked against the secondary key — but only when both the
    stored secondary signature AND the secondary key are available.
    Otherwise ``secondary_verified`` stays ``None``.
    """
    report = await session.get(Report, report_id)
    if report is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"report not found: {report_id}",
        )

    # No signature yet (e.g. report is mid-flight or just FAILED before
    # the SIGNING phase) — return a clean unverified response instead
    # of trying to verify an empty hex string and crashing.
    if not report.signature_hex:
        return VerifyResponse(
            report_id=report.id,
            verified=False,
            signature_hex=None,
            signature_secondary_hex=report.signature_secondary_hex,
            secondary_verified=None,
        )

    payload = await build_report_payload(
        session,
        framework=report.framework,
        period_start=report.period_start,
        period_end=report.period_end,
    )
    verified = verify_payload(payload, report.signature_hex, key=signing_key)

    secondary_verified: bool | None = None
    if report.signature_secondary_hex and secondary_signing_key is not None:
        secondary_verified = verify_payload(
            payload, report.signature_secondary_hex, key=secondary_signing_key
        )

    return VerifyResponse(
        report_id=report.id,
        verified=verified,
        signature_hex=report.signature_hex,
        signature_secondary_hex=report.signature_secondary_hex,
        secondary_verified=secondary_verified,
    )
