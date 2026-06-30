"""Integration tests for the C5 backfill path (``scripts.backfill_embeddings``)
and the ``embed_and_store_incident`` service helper.

Backfill is the recovery/migration path for incidents left ``embedding IS NULL``
(inserted before C5, or while the embedding service was down). These tests:

  * insert a NULL-embedded row via ``repository.add_incident(..., embedding=None)``
    and assert ``get_incidents_missing_embedding`` surfaces it;
  * run the batch backfill helper (``_backfill_one_batch``) → the row becomes a
    non-null 384-dim vector and is no longer "missing";
  * ``embed_and_store_incident`` on a NULL row → the row becomes non-null;
  * ``embed_and_store_incident`` on a missing id → ``None``.

The backfill *logic* is exercised through the session-injecting helpers
(``_backfill_one_batch``, ``embed_and_store_incident``), which take the test's
rolled-back ``db_session``. We deliberately do NOT call the module-level
``backfill()`` here: it opens its own ``get_session()`` (a separate real
connection) and would commit rows outside the test transaction, leaking state.
The full ``python -m scripts.backfill_embeddings`` entrypoint is covered by the
container E2E instead.

Run against the REAL migrated Postgres + baked model.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from src import services
from src.db import repository
from scripts import backfill_embeddings


@pytest.fixture
def unique() -> str:
    """Short unique suffix so rows don't collide across tests."""
    return uuid.uuid4().hex[:12]


def _add_null_embedded(session: Session, unique: str, **overrides: object):
    """Insert one incident with ``embedding=None`` and return it (flushed, PK set)."""
    kwargs = dict(
        title=f"Null-embedded {unique}",
        description="A row inserted without a vector; backfill should fix it.",
        service=f"legacy-{unique}",
        severity="medium",
        tags=["legacy", "no-vector"],
        resolution="n/a",
        embedding=None,
        commit=True,
    )
    kwargs.update(overrides)
    return repository.add_incident(session, **kwargs)


def _dims(session: Session, incident_id: int):
    """Return ``(embedding_is_not_null, vector_dims)`` for a row, via SQL."""
    return session.execute(
        text(
            "SELECT embedding IS NOT NULL, vector_dims(embedding) "
            "FROM incidents WHERE id = :id"
        ),
        {"id": incident_id},
    ).one()


# --------------------------------------------------------------------------- #
# get_incidents_missing_embedding finds NULL-embedded rows
# --------------------------------------------------------------------------- #
def test_missing_embedding_query_finds_null_row(
    db_session: Session, unique: str
) -> None:
    inc = _add_null_embedded(db_session, unique)
    assert inc.embedding is None

    missing = repository.get_incidents_missing_embedding(db_session, limit=500)
    assert inc.id in {m.id for m in missing}


# --------------------------------------------------------------------------- #
# The backfill batch loop populates NULL rows with a 384-dim vector
# --------------------------------------------------------------------------- #
def test_backfill_batch_populates_embedding(
    db_session: Session, unique: str
) -> None:
    inc = _add_null_embedded(db_session, unique)

    # Run one backfill batch against the test session (contained in the rolled-back
    # transaction). Batch size >= number of NULL rows in this session's view.
    n = backfill_embeddings._backfill_one_batch(db_session, batch_size=256)
    assert n >= 1  # at least our row was embedded

    non_null, dims = _dims(db_session, inc.id)
    assert non_null is True
    assert dims == 384

    # Our row is no longer reported as missing.
    still_missing = repository.get_incidents_missing_embedding(db_session, limit=500)
    assert inc.id not in {m.id for m in still_missing}


def test_backfill_batch_returns_zero_when_nothing_missing(
    db_session: Session, unique: str
) -> None:
    """A backfill batch drains everything, then reports 0 on the next pass."""
    _add_null_embedded(db_session, unique)
    # Drain all currently-missing rows.
    while backfill_embeddings._backfill_one_batch(db_session, batch_size=256) > 0:
        pass
    # Now nothing is missing → next batch embeds 0 rows.
    assert backfill_embeddings._backfill_one_batch(db_session, batch_size=256) == 0


# --------------------------------------------------------------------------- #
# embed_and_store_incident: NULL row → non-null; missing id → None
# --------------------------------------------------------------------------- #
def test_embed_and_store_incident_populates_null_row(
    db_session: Session, unique: str
) -> None:
    inc = _add_null_embedded(db_session, unique)
    assert inc.embedding is None

    updated = services.embed_and_store_incident(db_session, inc.id, commit=False)
    assert updated is not None
    assert updated.id == inc.id

    non_null, dims = _dims(db_session, inc.id)
    assert non_null is True
    assert dims == 384


def test_embed_and_store_incident_missing_id_returns_none(
    db_session: Session,
) -> None:
    assert services.embed_and_store_incident(db_session, 999_999_999) is None
