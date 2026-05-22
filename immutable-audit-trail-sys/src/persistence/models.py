"""SQLAlchemy ORM model for the immutable audit-records table.

The table is append-only at three layers:
  1. Application contract: only ChainAppender.append() writes rows.
  2. ORM contract: no update/delete methods are exposed on the model.
  3. Engine contract: BEFORE UPDATE / BEFORE DELETE triggers raise ABORT.

Defence-in-depth — even direct SQL through the same engine cannot mutate
existing rows. The only way past the triggers is to drop them, which
itself shows up in the chain (next verify call breaks).

NOTE on BEFORE vs AFTER triggers: the README/plan referenced
``AFTER UPDATE/AFTER DELETE`` triggers; we use BEFORE here because in
SQLite a BEFORE trigger aborts the offending statement *before* the row
mutation is even attempted, which is the more conventional shape for
"deny" triggers. RAISE(ABORT, ...) rolls back the transaction either
way, so callers observe identical behaviour — a
``sqlalchemy.exc.OperationalError`` with the message
``audit_records is append-only``.
"""
from __future__ import annotations

from sqlalchemy import Boolean, Float, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models in this project."""


class AuditRecord(Base):
    """One sealed audit record in the chain.

    seq=0 is the genesis row (inserted at init); seq>=1 is real activity.
    self_hash is UNIQUE so accidental duplicate writes fail loudly.
    """

    __tablename__ = "audit_records"

    seq: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    timestamp_utc: Mapped[str] = mapped_column(String(40), nullable=False)
    actor: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    resource: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    processing_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    args_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    result_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    prev_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    self_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    signature: Mapped[str] = mapped_column(String(128), nullable=False)

    __table_args__ = (
        Index("ix_audit_records_actor_ts", "actor", "timestamp_utc"),
        Index("ix_audit_records_action_ts", "action", "timestamp_utc"),
        Index("ix_audit_records_resource_ts", "resource", "timestamp_utc"),
    )


# --- Engine-enforced immutability ---------------------------------------------
#
# Applied once by ``init_db`` (added in C4) using ``CREATE TRIGGER IF NOT EXISTS``.
# Re-applying on every startup is idempotent — SQLite silently skips existing
# triggers when ``IF NOT EXISTS`` is set.
#
# The triggers fire BEFORE the offending statement, so the engine aborts
# the UPDATE/DELETE before the row mutation persists. RAISE(ABORT, ...)
# rolls the transaction back; the caller sees a
# ``sqlalchemy.exc.OperationalError`` with the message
# ``audit_records is append-only``.

IMMUTABILITY_TRIGGERS_SQL: tuple[str, ...] = (
    """
    CREATE TRIGGER IF NOT EXISTS audit_records_no_update
    BEFORE UPDATE ON audit_records
    BEGIN
        SELECT RAISE(ABORT, 'audit_records is append-only');
    END;
    """.strip(),
    """
    CREATE TRIGGER IF NOT EXISTS audit_records_no_delete
    BEFORE DELETE ON audit_records
    BEGIN
        SELECT RAISE(ABORT, 'audit_records is append-only');
    END;
    """.strip(),
)
