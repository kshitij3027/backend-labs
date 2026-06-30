"""Integration tests for embed-on-ingest (C5) at the ``POST /incidents`` boundary.

From C5 the router computes the MiniLM embedding on ingest and persists it with
the row, so the response carries ``has_embedding: true`` and the stored row has a
non-null ``vector(384)``. If the embedding service raises, the request must fail
loudly with **HTTP 503** ("embedding service unavailable") rather than silently
persisting a NULL-embedded (unsearchable) row.

Run against the REAL migrated Postgres + baked model (same ``db_session`` /
overridden ``get_db`` pattern as ``test_incidents_api.py`` — HTTP writes land in a
transaction that is rolled back on teardown).
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api import create_app
from src.db.session import get_db
from src.routers import incidents as incidents_router


@pytest.fixture
def unique() -> str:
    """Short unique suffix so rows don't collide across tests."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``."""
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _payload(**overrides: object) -> dict:
    body = {
        "title": "Kafka consumer lag spiking on the ingest topic",
        "description": "Consumer group fell behind; end-to-end latency breached SLO.",
        "service": "ingest-worker",
        "severity": "high",
        "tags": ["kafka", "lag", "latency"],
        "resolution": "Scaled the consumer group and raised max.poll.records.",
    }
    body.update(overrides)
    return body


# --------------------------------------------------------------------------- #
# Happy path: embedded on ingest
# --------------------------------------------------------------------------- #
def test_post_incident_embeds_on_ingest(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """POST → 201, ``has_embedding: true``, ``"embedding"`` absent from the JSON,
    and the persisted row has a non-null 384-dim vector."""
    resp = client.post("/incidents", json=_payload(service=f"ingest-{unique}"))
    assert resp.status_code == 201, resp.text
    body = resp.json()

    assert isinstance(body["id"], int)
    assert body["has_embedding"] is True
    # The raw vector must never be serialised on the wire.
    assert "embedding" not in body

    row = db_session.execute(
        text(
            "SELECT embedding IS NOT NULL, vector_dims(embedding) "
            "FROM incidents WHERE id = :id"
        ),
        {"id": body["id"]},
    ).one()
    assert row[0] is True, "persisted embedding should be non-null"
    assert row[1] == 384, "persisted embedding should be 384-dim"


# --------------------------------------------------------------------------- #
# Embedding-service-down path → 503
# --------------------------------------------------------------------------- #
def test_post_incident_returns_503_when_embedding_unavailable(
    client: TestClient, db_session: Session, unique: str, monkeypatch
) -> None:  # noqa: ANN001
    """If ``embed_incident`` raises, POST → 503 "embedding service unavailable"
    and no row is persisted (the failure precedes the insert)."""

    def _boom(*_args: object, **_kwargs: object):
        raise RuntimeError("model exploded")

    # The router calls ``embeddings.embed_incident`` via the module-level
    # ``embeddings`` symbol it imported, so patch that attribute.
    monkeypatch.setattr(
        incidents_router.embeddings, "embed_incident", _boom
    )

    svc = f"down-{unique}"
    resp = client.post("/incidents", json=_payload(service=svc))
    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "embedding service unavailable"

    # Nothing should have been written for this service.
    count = db_session.execute(
        text("SELECT count(*) FROM incidents WHERE service = :svc"),
        {"svc": svc},
    ).scalar()
    assert count == 0, "a failed embedding must not persist a NULL-embedded row"
