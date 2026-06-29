"""SQLAlchemy 2.0 typed ORM models for the Log Recommendation Engine.

Four tables back the system (see the §"Design deep-dive" in ``plan.md``):

* :class:`Incident` — the incident corpus a new incident is matched against. Each
  row carries its text (title/description/resolution), contextual metadata
  (service/severity/tags), and a dense ``embedding`` (``vector(384)``). The
  embedding is populated later (C5); in C2 it is left ``NULL``. An HNSW cosine
  index over ``embedding`` (built in the initial migration) powers ANN retrieval.
* :class:`Recommendation` — a served query: the submitted query plus the returned
  suggestion incident ids (``query_json``), so later feedback can reference a real
  prior result.
* :class:`Feedback` — one engineer vote (helpful / not-helpful) on a suggested
  incident for a given served recommendation and query-pattern.
* :class:`SuggestionScore` — the learned aggregate: helpful/unhelpful counts per
  ``(query_pattern, incident_id)``, upserted on every feedback event. This is the
  signal the feedback-driven re-ranking (C11) reads.

All datetime columns are timezone-aware (``DateTime(timezone=True)``). JSON
payloads use PostgreSQL ``JSONB`` and tag lists use the PostgreSQL ``ARRAY`` type
(the deployment target is Postgres+pgvector).

The embedding dimension is fixed at :data:`EMBEDDING_DIM` (384, the output size of
``all-MiniLM-L6-v2``); it is kept in sync with ``Settings.embedding_dim``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base

# Dense-embedding dimensionality (all-MiniLM-L6-v2 → 384). Fixed at the schema
# level: the ``vector(N)`` column and the HNSW index are sized to this. Kept in
# sync with ``src.config.Settings.embedding_dim``.
EMBEDDING_DIM = 384


class Incident(Base):
    """A historical incident: text + contextual metadata + a dense embedding."""

    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    service: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    severity: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    # Free-form labels (e.g. ["timeout", "db"]); Postgres text[] with a [] default.
    tags: Mapped[list[str]] = mapped_column(
        ARRAY(String), nullable=False, default=list
    )
    resolution: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
        nullable=False,
        server_default=func.now(),
    )
    # Populated in C5 (embedding service). Nullable in C2 — no vectors computed yet.
    # The HNSW cosine index over this column is created in the initial migration.
    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(EMBEDDING_DIM), nullable=True
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Incident(id={self.id!r}, title={self.title!r}, "
            f"service={self.service!r}, severity={self.severity!r})"
        )


class Recommendation(Base):
    """A served recommendation: the query and the suggestion ids it returned."""

    __tablename__ = "recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Stable hash of the normalized query — lets a repeated query find its prior
    # served result (and the cache) quickly.
    query_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    # The submitted query plus the returned suggestion incident ids (and any score
    # breakdown), so feedback can reference a real prior result.
    query_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Recommendation(id={self.id!r}, query_hash={self.query_hash!r}, "
            f"created_at={self.created_at!r})"
        )


class Feedback(Base):
    """One helpful / not-helpful vote on a suggested incident."""

    __tablename__ = "feedback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recommendation_id: Mapped[int] = mapped_column(
        ForeignKey("recommendations.id"), index=True, nullable=False
    )
    incident_id: Mapped[int] = mapped_column(
        ForeignKey("incidents.id"), index=True, nullable=False
    )
    # Normalized query-pattern bucket (e.g. "service|severity|sorted-tags"); the key
    # the learned aggregate is grouped by.
    query_pattern: Mapped[str] = mapped_column(
        String(256), index=True, nullable=False
    )
    helpful: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Feedback(id={self.id!r}, recommendation_id={self.recommendation_id!r}, "
            f"incident_id={self.incident_id!r}, query_pattern={self.query_pattern!r}, "
            f"helpful={self.helpful!r})"
        )


class SuggestionScore(Base):
    """Learned aggregate of feedback per ``(query_pattern, incident_id)``.

    Upserted on each feedback event; read by the feedback-driven re-ranking to
    boost / dampen a suggestion for a given query-pattern.
    """

    __tablename__ = "suggestion_scores"

    query_pattern: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False
    )
    incident_id: Mapped[int] = mapped_column(
        ForeignKey("incidents.id"), primary_key=True, nullable=False
    )
    helpful_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unhelpful_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"SuggestionScore(query_pattern={self.query_pattern!r}, "
            f"incident_id={self.incident_id!r}, helpful_count={self.helpful_count!r}, "
            f"unhelpful_count={self.unhelpful_count!r})"
        )
