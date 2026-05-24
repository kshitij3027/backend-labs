"""SQLAlchemy ORM models for the GDPR erasure system.

Three tables back the whole pipeline:

  * ``UserDataMapping`` — the data-lineage tracker. One row per place a
    given user's data lives: ``(user_id, data_type, storage_location,
    data_path)``. The coordinator's DISCOVERING phase queries this
    table to find every location it needs to touch.
  * ``ErasureRequest`` — the lifecycle state machine for one erasure
    request. Drives ``PENDING -> DISCOVERING -> EXECUTING -> VERIFYING
    -> COMPLETED`` (or ``FAILED`` as a terminal error state).
  * ``ErasureAuditLog`` — the immutable, append-only hash-chained log
    of every state transition and per-location action. The genesis row
    has ``sequence=0`` and ``request_id=NULL``; every later row points
    back to the previous row's ``entry_hash`` via ``prev_hash``.

JSON columns use ``JSON().with_variant(JSONB(), "postgresql")`` so the
same schema works in both the SQLite test database (plain ``JSON`` /
``TEXT`` underneath) and PostgreSQL (native ``JSONB``).
"""
from __future__ import annotations

import datetime as dt
import enum
import uuid
from typing import Any, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from src.persistence.db import Base


def _utcnow() -> dt.datetime:
    """Naive UTC timestamp with microseconds truncated.

    Matches the sibling ``automated-log-retention`` convention so all
    timestamps round-trip cleanly through SQLite (which doesn't carry
    tz info) and Postgres (which would, but we keep it naive for
    cross-driver parity).
    """
    return dt.datetime.utcnow().replace(microsecond=0)


def _new_uuid() -> str:
    return str(uuid.uuid4())


# JSONB on Postgres (production), JSON on SQLite (tests). One ``Column``
# spec, two storage backends, zero schema drift.
_JsonType = JSON().with_variant(JSONB(), "postgresql")


class RequestType(str, enum.Enum):
    """What the coordinator should do with the user's data."""

    DELETE = "DELETE"
    ANONYMIZE = "ANONYMIZE"


class RequestState(str, enum.Enum):
    """Lifecycle states for an ``ErasureRequest``.

    The flow is ``PENDING -> DISCOVERING -> EXECUTING -> VERIFYING ->
    COMPLETED``; ``FAILED`` is a terminal error state reachable from
    any non-terminal state.
    """

    PENDING = "PENDING"
    DISCOVERING = "DISCOVERING"
    EXECUTING = "EXECUTING"
    VERIFYING = "VERIFYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class UserDataMapping(Base):
    """One known location of a single user's data.

    The unique constraint covers ``(user_id, data_type,
    storage_location, data_path)``. Note Postgres treats ``NULL`` as
    distinct in unique constraints by default, which is intentional
    here: two rows with the same user/type/location and both
    ``data_path=NULL`` represent two conceptually different "default"
    locations and should both be allowed. Once a row carries a concrete
    ``data_path``, the constraint blocks duplicate inserts at the same
    path. SQLite's default behaviour matches Postgres on this point.
    """

    __tablename__ = "user_data_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    data_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    storage_location: Mapped[str] = mapped_column(String(255), nullable=False)
    data_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    metadata_json: Mapped[Optional[dict[str, Any]]] = mapped_column(
        _JsonType, nullable=True
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=_utcnow, nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "data_type",
            "storage_location",
            "data_path",
            name="uq_user_data_location",
        ),
    )


class ErasureRequest(Base):
    """One user-facing erasure request and its lifecycle.

    ``id`` is a UUID string (PK) so external systems (e.g. the
    dashboard, regulator-facing tooling) can mint and reference IDs
    without coordinating with the DB. ``state`` is indexed because the
    coordinator polls for non-terminal rows on every tick.
    """

    __tablename__ = "erasure_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_uuid)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    request_type: Mapped[RequestType] = mapped_column(
        SAEnum(RequestType, name="request_type"), nullable=False
    )
    state: Mapped[RequestState] = mapped_column(
        SAEnum(RequestState, name="request_state"),
        default=RequestState.PENDING,
        nullable=False,
        index=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=_utcnow, nullable=False
    )
    started_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, nullable=True)

    audit_entries: Mapped[list["ErasureAuditLog"]] = relationship(
        back_populates="request",
        order_by="ErasureAuditLog.sequence",
        cascade="all, delete-orphan",
    )


class ErasureAuditLog(Base):
    """One sealed entry in the SHA-256 hash chain.

    ``sequence`` is globally unique across the whole table (not scoped
    per request) so the chain is a single linear history; the verifier
    in commit 5 walks rows by ``sequence`` ascending. ``request_id`` is
    nullable because the genesis row (``sequence=0``) belongs to no
    request, and ``ON DELETE SET NULL`` keeps the chain intact if a
    request row is ever pruned.
    """

    __tablename__ = "erasure_audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("erasure_requests.id", ondelete="SET NULL"),
        nullable=True,  # genesis row has request_id=NULL
        index=True,
    )
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(_JsonType, nullable=False)
    prev_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime, default=_utcnow, nullable=False
    )

    request: Mapped[Optional["ErasureRequest"]] = relationship(
        back_populates="audit_entries"
    )

    __table_args__ = (
        Index("ix_audit_request_sequence", "request_id", "sequence"),
    )
