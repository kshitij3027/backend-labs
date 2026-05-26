"""Pydantic v2 request and response schemas shared across API routers.

Every model here either accepts a request body or shapes a response.
Models built from ORM rows opt into ``from_attributes=True`` so
``ReportStatusResponse.model_validate(report_orm_row)`` Just Works
without a hand-written ``_serialize`` helper.

The schema module is deliberately framework-validation-free: the
``framework`` string on :class:`GenerateReportRequest` is validated
inside the route handler against the live ``FRAMEWORK_REGISTRY``, not
here, to avoid an import-time circular (``frameworks`` -> ``api`` ->
``frameworks`` would happen if we imported the registry up here).
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict


# ── Generate-report request / response ──────────────────────────────


class GenerateReportRequest(BaseModel):
    """Body for ``POST /reports/generate``.

    ``framework`` is validated inside the route against
    ``FRAMEWORK_REGISTRY`` (see :mod:`src.api.routes_reports`) — not
    here, because importing the registry at schema-module load time
    would force ``src.frameworks`` to import before ``src.api``, which
    is the wrong direction.
    """

    framework: str
    period_start: datetime
    period_end: datetime
    export_format: Literal["PDF", "CSV", "JSON", "XML"]
    title: Optional[str] = None
    description: Optional[str] = None


class GenerateReportResponse(BaseModel):
    """202 body returned from ``POST /reports/generate``.

    The state is always ``"PENDING"`` at this point; callers poll
    ``GET /reports/{id}`` for the live state machine.
    """

    report_id: UUID
    state: str


# ── Report status / verify ──────────────────────────────────────────


class ReportStatusResponse(BaseModel):
    """Full status row for ``GET /reports/{id}``.

    ``download_url`` is populated only when the report has reached
    ``COMPLETED``; the verify URL is always present so callers can
    fall back to a signature check even mid-flight (it returns
    ``verified=False`` until the signature has been computed).
    """

    model_config = ConfigDict(from_attributes=True)

    report_id: UUID
    state: str
    framework: str
    export_format: str
    period_start: datetime
    period_end: datetime
    created_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    download_url: Optional[str] = None
    verify_url: str
    signature_hex: Optional[str] = None
    signature_secondary_hex: Optional[str] = None


class VerifyResponse(BaseModel):
    """Response shape for ``GET /reports/{id}/verify``.

    ``secondary_verified`` stays ``None`` for single-signature reports.
    It only carries a bool when ``signature_secondary_hex`` is set on
    the row (FinHealth dual-sign), so callers can tell "no secondary
    signature ever existed" from "secondary signature exists but is
    invalid".
    """

    report_id: UUID
    verified: bool
    signature_hex: Optional[str] = None
    signature_secondary_hex: Optional[str] = None
    secondary_verified: Optional[bool] = None


# ── Framework catalogue ─────────────────────────────────────────────


class FrameworkInfo(BaseModel):
    """One entry returned from ``GET /frameworks``."""

    name: str
    categories: list[str]
    description: str


# ── Dashboard stats ─────────────────────────────────────────────────


class RecentReportSummary(BaseModel):
    """Compact row used in the ``recent`` list of :class:`DashboardStats`.

    Built from a ``Report`` ORM row via ``from_attributes=True``; the
    stats service renames the row's ``id`` -> ``report_id`` so the
    field names on the API response are explicit.
    """

    model_config = ConfigDict(from_attributes=True)

    report_id: UUID
    framework: str
    export_format: str
    state: str
    created_at: datetime
    completed_at: Optional[datetime] = None


class DashboardStats(BaseModel):
    """Top-level aggregates rendered on the dashboard.

    ``success_rate`` is COMPLETED / (COMPLETED + FAILED). It excludes
    in-flight reports — a report that's still aggregating shouldn't
    pull the rate down. When no terminal reports exist yet the rate
    is reported as ``0.0`` (callers should treat this as "n/a", not
    "100% failure"; the dashboard surfaces this via the in-flight
    counter).
    """

    total_reports: int
    framework_breakdown: dict[str, int]
    format_breakdown: dict[str, int]
    success_rate: float
    in_flight: int
    recent: list[RecentReportSummary]
