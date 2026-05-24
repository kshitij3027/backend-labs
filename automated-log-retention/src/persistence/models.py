"""SQLAlchemy ORM models for the automated-log-retention project.

Five tables back the whole pipeline:

  * ``files`` — the source of truth for every segment under management.
    The lifecycle code never walks the filesystem; it queries this table.
  * ``transitions`` — planned moves between tiers (scanner inserts;
    applier executes).
  * ``pending_deletes`` — mark-then-sweep queue for hard deletes.
  * ``audit_entries`` — append-only SHA-256 hash chain. Genesis row
    (seq=0) is inserted in C13 by the appender (not here in C02), so
    ``seq`` uses ``autoincrement=False`` to allow the explicit value.
  * ``job_runs`` — one row per scheduler job invocation (scan/apply/
    sweep/verify_chain), captured for the dashboard's recency display.

All timestamps are stored as Python ``datetime`` (naive UTC). The app
layer guarantees ``datetime.now(timezone.utc).replace(tzinfo=None)`` so
SQLite gets back the same string format every time without timezone
suffixes. Enum-like fields (``tier``, ``action``, ``status``) are
``String`` — validation lives in the policy/Pydantic layer, not the DB.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models in this project."""


class File(Base):
    """One log segment under retention management.

    A ``File`` row is created by the ingest path when a segment is
    closed (rolled) and registered. The lifecycle scanner picks rows
    whose ``next_eval_at`` is due; the applier mutates ``tier``,
    ``segment_path``, and ``size_bytes`` after each successful move.
    """

    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    segment_path: Mapped[str] = mapped_column(String(1024), nullable=False, unique=True)
    tier: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    oldest_record_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    newest_record_ts: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    compliance_tag: Mapped[str | None] = mapped_column(String(32), nullable=True)
    immutable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    next_eval_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class Transition(Base):
    """A planned (or executed) lifecycle move for a single ``File``.

    Status flows ``pending`` -> ``applied`` (success) or
    ``pending`` -> ``failed`` (with ``error`` populated). The applier
    walks rows with ``status='pending'`` ordered by ``planned_at``.
    """

    __tablename__ = "transitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("files.id"), nullable=False, index=True
    )
    from_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    to_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    planned_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class PendingDelete(Base):
    """A file that has been moved into ``tiers/pending/`` and is awaiting
    the sweeper. ``delete_after`` is the earliest time the sweeper may
    unlink the file (a grace window for recovery if a bug is caught).
    """

    __tablename__ = "pending_deletes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("files.id"), nullable=False
    )
    path: Mapped[str] = mapped_column(String(1024), nullable=False)
    delete_after: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


class AuditEntry(Base):
    """One sealed entry in the SHA-256 hash chain.

    ``seq`` is explicit (not autoincrement) — the appender computes the
    next seq under a SQLite ``BEGIN IMMEDIATE`` lock so the chain links
    deterministically. ``metadata_json`` is a JSON string (not a native
    SQLite JSON column) to keep the schema portable and to ensure the
    canonicalisation step in the appender controls the byte order.

    ``entry_hash`` is UNIQUE so accidental duplicate writes fail loudly
    at the engine layer rather than silently corrupting the chain.
    """

    __tablename__ = "audit_entries"

    seq: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    ts_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    actor: Mapped[str] = mapped_column(String(255), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource: Mapped[str] = mapped_column(String(512), nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False)
    prev_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)

    __table_args__ = (
        Index("ix_audit_entries_actor_ts", "actor", "ts_utc"),
    )


class JobRun(Base):
    """One scheduler job invocation. Used by the dashboard to display the
    most recent scan/apply/sweep/verify times and outcomes."""

    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    summary_json: Mapped[str | None] = mapped_column(Text, nullable=True)
