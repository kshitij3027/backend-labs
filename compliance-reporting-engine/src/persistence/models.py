"""SQLAlchemy ORM models for the Multi-Framework Compliance Reporting Engine.

Three tables back the whole pipeline:

  * ``LogEvent`` — one normalised compliance-relevant event ingested
    from upstream log sources. Each event carries a list of
    ``framework_tags`` (SOX, HIPAA, PCI_DSS, GDPR, ...) so the
    aggregator can filter without re-classifying. The free-form
    ``payload`` JSON column carries any framework-specific extras the
    exporters might want to render verbatim.
  * ``Report`` — the lifecycle row for one generated report. Drives
    ``PENDING -> RUNNING -> COMPLETED`` (or ``FAILED``); also holds
    the dual HMAC signatures (primary + secondary key rotation) and
    the export format so the dashboard can hand the right file back
    to a downloader.
  * ``ReportFile`` — one rendered artefact per report (a single PDF,
    CSV, etc.). Most reports yield a single file, but the schema
    leaves room for multi-artefact bundles (e.g. PDF + companion CSV).
    ``ON DELETE CASCADE`` keeps the table clean when a report row is
    pruned.

The ``GUID`` ``TypeDecorator`` and ``JSONType`` variant trick let the
same schema run against native Postgres types in production
(``UUID`` + ``JSONB``) and SQLite-friendly equivalents (``CHAR(36)`` +
``JSON``) in the unit-test suite — zero schema drift, zero
"works on Postgres only" surprises.

All datetime columns are ``TZDateTime()`` and the
``_utcnow()`` helper returns timezone-aware UTC, matching the
project's "everything in UTC, always aware" convention.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import CHAR, TypeDecorator


class Base(DeclarativeBase):
    """Declarative base for all ORM models in this project."""


# --- Cross-dialect type helpers -----------------------------------------------


class GUID(TypeDecorator):
    """UUID column that uses native ``UUID`` on Postgres, ``CHAR(36)`` on SQLite.

    Without this, the unit tests (in-memory SQLite) couldn't share the
    same models as the production Postgres deployment. ``cache_ok`` is
    safe to flip on because the implementation has no per-instance
    state.
    """

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if dialect.name == "postgresql":
            return value if isinstance(value, UUID) else UUID(str(value))
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value if isinstance(value, UUID) else UUID(str(value))


# JSONB on Postgres (production), JSON on SQLite (tests). One column
# spec, two storage backends, no schema drift.
JSONType = JSON().with_variant(JSONB(), "postgresql")


class TZDateTime(TypeDecorator):
    """Always-tz-aware DateTime column.

    SQLite (used by the unit-test suite via aiosqlite) silently strips
    ``tzinfo`` from values on read because it has no native ``TIMESTAMPTZ``
    type. Postgres preserves the offset. To keep round-tripping consistent
    across both backends, this decorator re-attaches UTC to any naive
    datetime coming back from the driver. Production-side Postgres values
    are already aware and pass through unchanged.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


def _utcnow() -> datetime:
    """Timezone-aware UTC ``now()`` — used as the default for created/updated columns."""
    return datetime.now(timezone.utc)


# --- Models -------------------------------------------------------------------


class LogEvent(Base):
    """One normalised compliance-relevant log event.

    ``framework_tags`` is a JSON list of framework codes that this event
    matters to (e.g. ``["SOX", "HIPAA"]``); a single event can belong
    to multiple frameworks. ``payload`` carries any extra fields the
    framework exporters might want to render verbatim without forcing a
    schema migration every time a new field shows up.
    """

    __tablename__ = "log_events"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    framework_tags: Mapped[list[str]] = mapped_column(JSONType, default=list)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    actor: Mapped[str] = mapped_column(String(255))
    resource: Mapped[str] = mapped_column(String(255))
    action: Mapped[str] = mapped_column(String(64))
    outcome: Mapped[str] = mapped_column(String(32))            # success | failure | denied
    sensitivity: Mapped[str] = mapped_column(String(32))        # public | internal | confidential | restricted
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, default=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict for JSON exporters / API responses."""
        return {
            "id": str(self.id),
            "timestamp": self.timestamp.isoformat(),
            "framework_tags": list(self.framework_tags or []),
            "event_type": self.event_type,
            "actor": self.actor,
            "resource": self.resource,
            "action": self.action,
            "outcome": self.outcome,
            "sensitivity": self.sensitivity,
            "payload": dict(self.payload or {}),
        }


class Report(Base):
    """One generated compliance report and its lifecycle row.

    ``state`` follows ``PENDING -> RUNNING -> COMPLETED`` (or ``FAILED``
    as a terminal error state) and is indexed because the dashboard
    polls non-terminal rows on every refresh. The dual signature columns
    (``signature_hex`` + ``signature_secondary_hex``) support HMAC key
    rotation: a secondary key may be present during a rotation window,
    so a verifier accepts either.
    """

    __tablename__ = "reports"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    framework: Mapped[str] = mapped_column(String(32), index=True)
    period_start: Mapped[datetime] = mapped_column(TZDateTime())
    period_end: Mapped[datetime] = mapped_column(TZDateTime())
    export_format: Mapped[str] = mapped_column(String(8))     # PDF | CSV | JSON | XML
    state: Mapped[str] = mapped_column(String(16), index=True, default="PENDING")
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    signature_hex: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    signature_secondary_hex: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime(), default=_utcnow, onupdate=_utcnow
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(TZDateTime(), nullable=True)

    files: Mapped[list["ReportFile"]] = relationship(
        "ReportFile", back_populates="report", cascade="all, delete-orphan"
    )


class ReportFile(Base):
    """One rendered artefact (PDF, CSV, ...) belonging to a report.

    ``ON DELETE CASCADE`` keeps the table clean when a parent
    ``Report`` row is pruned. ``encrypted`` records whether the bytes
    on disk are wrapped by the Fernet key — separate from the HMAC
    signatures, which sign the un-encrypted content.
    """

    __tablename__ = "report_files"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    report_id: Mapped[UUID] = mapped_column(
        GUID(), ForeignKey("reports.id", ondelete="CASCADE"), index=True
    )
    file_path: Mapped[str] = mapped_column(String(512))
    format: Mapped[str] = mapped_column(String(8))
    size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    encrypted: Mapped[bool] = mapped_column(Boolean, default=False)

    report: Mapped["Report"] = relationship("Report", back_populates="files")
