"""Integration tests for the C2 persistence layer.

Run against the REAL PostgreSQL+pgvector service (the migrated schema, the
``vector`` extension, the ``vector(384)`` column and the HNSW index cannot be
faked with SQLite). The session-scoped ``_migrated_db`` fixture (in
``conftest.py``) applies ``alembic upgrade head`` first, then each test runs in a
rolled-back transaction so writes never leak.

Coverage:
  * the ``vector`` extension is installed (the migration created it);
  * the ``incidents`` table and its HNSW index exist;
  * ``add_incident`` → ``get_incident`` round-trips every field (tags list, NULL
    embedding) faithfully;
  * ``list_incidents`` filters by ``service`` / ``severity``.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import repository as repo
from src.db.models import EMBEDDING_DIM


@pytest.fixture
def unique() -> str:
    """A short unique suffix to namespace rows per test (safe reruns/isolation)."""
    return uuid.uuid4().hex[:12]


# --------------------------------------------------------------------------- #
# Schema-level assertions (extension + table + HNSW index)
# --------------------------------------------------------------------------- #
def test_vector_extension_installed(raw_connection) -> None:  # noqa: ANN001
    """The initial migration ran ``CREATE EXTENSION IF NOT EXISTS vector``."""
    result = raw_connection.execute(
        text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
    ).scalar()
    assert result == 1


def test_incidents_table_exists(raw_connection) -> None:  # noqa: ANN001
    """The ``incidents`` table was created by the migration."""
    result = raw_connection.execute(
        text("SELECT to_regclass('public.incidents')")
    ).scalar()
    assert result == "incidents"


def test_hnsw_index_exists(raw_connection) -> None:  # noqa: ANN001
    """The HNSW cosine ANN index over ``incidents.embedding`` exists."""
    row = raw_connection.execute(
        text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE tablename = 'incidents' "
            "AND indexname = 'ix_incidents_embedding_hnsw'"
        )
    ).first()
    assert row is not None, "ix_incidents_embedding_hnsw index is missing"
    indexdef = row[0].lower()
    assert "using hnsw" in indexdef
    assert "vector_cosine_ops" in indexdef


def test_embedding_dim_constant_is_384() -> None:
    """The embedding dimension is fixed at 384 (all-MiniLM-L6-v2)."""
    assert EMBEDDING_DIM == 384


# --------------------------------------------------------------------------- #
# add_incident / get_incident round-trip
# --------------------------------------------------------------------------- #
def test_add_and_get_incident_roundtrip(db_session: Session, unique: str) -> None:
    service = f"payments_{unique}"
    tags = ["timeout", "db", "p1"]

    created = repo.add_incident(
        db_session,
        title=f"Checkout 500s {unique}",
        description="Checkout returned 500 under load; DB pool exhausted.",
        service=service,
        severity="high",
        tags=tags,
        resolution="Raised the connection pool ceiling and added a circuit breaker.",
        commit=True,
    )
    assert created.id is not None  # PK assigned

    fetched = repo.get_incident(db_session, created.id)
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.title == f"Checkout 500s {unique}"
    assert fetched.description.startswith("Checkout returned 500")
    assert fetched.service == service
    assert fetched.severity == "high"
    # The Postgres text[] tags list survives the round-trip, order preserved.
    assert fetched.tags == tags
    assert fetched.resolution.startswith("Raised the connection pool")
    # created_at is server-defaulted + timezone-aware.
    assert fetched.created_at is not None
    assert fetched.created_at.tzinfo is not None
    # embedding stays NULL in C2 (no vectors computed yet).
    assert fetched.embedding is None


def test_add_incident_defaults_empty_tags(db_session: Session, unique: str) -> None:
    created = repo.add_incident(
        db_session,
        title=f"No tags {unique}",
        description="An incident without tags.",
        service=f"auth_{unique}",
        severity="low",
        resolution="Restarted the service.",
        commit=True,
    )
    fetched = repo.get_incident(db_session, created.id)
    assert fetched is not None
    assert fetched.tags == []  # ARRAY default is an empty list, not NULL


def test_get_incident_missing_returns_none(db_session: Session) -> None:
    assert repo.get_incident(db_session, 999_999_999) is None


# --------------------------------------------------------------------------- #
# list_incidents filtering
# --------------------------------------------------------------------------- #
def test_list_incidents_filters_by_service_and_severity(
    db_session: Session, unique: str
) -> None:
    svc_a = f"orders_{unique}"
    svc_b = f"search_{unique}"

    # Two incidents in svc_a (one high, one low), one in svc_b (high).
    repo.add_incident(
        db_session,
        title="A-high",
        description="d",
        service=svc_a,
        severity="high",
        tags=["x"],
        resolution="r",
        commit=True,
    )
    repo.add_incident(
        db_session,
        title="A-low",
        description="d",
        service=svc_a,
        severity="low",
        tags=[],
        resolution="r",
        commit=True,
    )
    repo.add_incident(
        db_session,
        title="B-high",
        description="d",
        service=svc_b,
        severity="high",
        tags=[],
        resolution="r",
        commit=True,
    )

    # Filter by service.
    a_rows = repo.list_incidents(db_session, service=svc_a)
    assert {r.title for r in a_rows} == {"A-high", "A-low"}
    assert all(r.service == svc_a for r in a_rows)

    # Filter by service + severity.
    a_high = repo.list_incidents(db_session, service=svc_a, severity="high")
    assert {r.title for r in a_high} == {"A-high"}

    # Filter by severity alone still surfaces our unique-service rows.
    high_rows = repo.list_incidents(db_session, severity="high", limit=500)
    titles = {r.title for r in high_rows if r.service in {svc_a, svc_b}}
    assert titles == {"A-high", "B-high"}


def test_list_incidents_respects_limit(db_session: Session, unique: str) -> None:
    svc = f"cache_{unique}"
    for i in range(5):
        repo.add_incident(
            db_session,
            title=f"C-{i}",
            description="d",
            service=svc,
            severity="medium",
            tags=[],
            resolution="r",
            commit=True,
        )
    limited = repo.list_incidents(db_session, service=svc, limit=3)
    assert len(limited) == 3
