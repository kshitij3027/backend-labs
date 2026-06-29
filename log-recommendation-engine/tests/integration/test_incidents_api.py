"""Integration tests for the C3 incident-corpus API (:mod:`src.routers.incidents`).

Run against the REAL migrated Postgres (the ``db_session`` fixture in
``conftest.py`` gives a transaction that is rolled back on teardown). The FastAPI
``get_db`` dependency is overridden to yield that same session so writes the
``TestClient`` makes through HTTP are visible to the test and are undone
afterwards.

Coverage:
  * ``POST /incidents`` valid → **201**, body ``has_embedding == False``;
  * the persisted row's ``embedding IS NULL`` (queried back via the session);
  * ``GET /incidents/{id}`` round-trips it; unknown id → **404**;
  * ``GET /incidents?service=`` / ``?severity=`` filter correctly and ``total``
    reflects the full match count (not just the page);
  * blank / invalid-severity bodies → **422**.
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


@pytest.fixture
def unique() -> str:
    """Short unique suffix so filter tests don't collide with other rows."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Overriding the dependency means every request in a test runs inside the same
    outer transaction as ``db_session``, so HTTP writes are visible to direct
    session queries here and are discarded when the fixture tears down.
    """
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _payload(**overrides: object) -> dict:
    body = {
        "title": "Database connection pool exhausted",
        "description": "Requests timed out waiting on a checked-out DB connection.",
        "service": "orders-api",
        "severity": "high",
        "tags": ["db", "timeout"],
        "resolution": "Raised max pool size and added a statement timeout.",
    }
    body.update(overrides)
    return body


# --------------------------------------------------------------------------- #
# POST /incidents
# --------------------------------------------------------------------------- #
def test_post_incident_returns_201_no_embedding(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A valid POST → 201, ``has_embedding == False``, and the row is NULL-embedded."""
    resp = client.post("/incidents", json=_payload(service=f"orders-{unique}"))
    assert resp.status_code == 201, resp.text
    body = resp.json()

    assert isinstance(body["id"], int)
    assert body["service"] == f"orders-{unique}"
    assert body["severity"] == "high"
    assert body["tags"] == ["db", "timeout"]
    assert body["has_embedding"] is False
    # The vector is never serialised.
    assert "embedding" not in body

    # Confirm the *persisted* row has a NULL embedding (C3 never computes vectors).
    embedding = db_session.execute(
        text("SELECT embedding FROM incidents WHERE id = :id"),
        {"id": body["id"]},
    ).scalar()
    assert embedding is None


def test_post_incident_trims_and_dedupes_tags(
    client: TestClient, unique: str
) -> None:
    """Tag cleaning applies through the HTTP boundary too."""
    resp = client.post(
        "/incidents",
        json=_payload(
            service=f"svc-{unique}", tags=[" db ", "db", "timeout", "", "timeout"]
        ),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["tags"] == ["db", "timeout"]


@pytest.mark.parametrize(
    "bad_body",
    [
        {"title": ""},
        {"title": "   "},
        {"description": ""},
        {"resolution": "  "},
        {"service": ""},
        {"severity": "urgent"},
        {"severity": "SEV1"},
    ],
)
def test_post_invalid_body_returns_422(
    client: TestClient, bad_body: dict
) -> None:
    """Blank required text or an out-of-set severity → 422."""
    resp = client.post("/incidents", json=_payload(**bad_body))
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------- #
# GET /incidents/{id}
# --------------------------------------------------------------------------- #
def test_get_incident_by_id_roundtrips(
    client: TestClient, unique: str
) -> None:
    """A created incident is retrievable by its id with the same content."""
    created = client.post(
        "/incidents", json=_payload(service=f"svc-{unique}", title=f"T-{unique}")
    ).json()
    resp = client.get(f"/incidents/{created['id']}")
    assert resp.status_code == 200, resp.text
    fetched = resp.json()
    assert fetched["id"] == created["id"]
    assert fetched["title"] == f"T-{unique}"
    assert fetched["service"] == f"svc-{unique}"
    assert fetched["has_embedding"] is False


def test_get_unknown_incident_returns_404(client: TestClient) -> None:
    """An unknown id → 404."""
    resp = client.get("/incidents/999999999")
    assert resp.status_code == 404, resp.text


# --------------------------------------------------------------------------- #
# GET /incidents (filtering + total)
# --------------------------------------------------------------------------- #
def test_list_filters_by_service_and_reports_total(
    client: TestClient, unique: str
) -> None:
    """``?service=`` filters to matching rows and ``total`` is the full match count."""
    svc = f"filtersvc-{unique}"
    # 3 in our unique service (2 high, 1 low), plus 1 in a different service.
    for sev in ("high", "high", "low"):
        assert (
            client.post("/incidents", json=_payload(service=svc, severity=sev)).status_code
            == 201
        )
    assert (
        client.post(
            "/incidents", json=_payload(service=f"other-{unique}", severity="high")
        ).status_code
        == 201
    )

    resp = client.get(f"/incidents?service={svc}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    assert all(item["service"] == svc for item in data["items"])


def test_list_filters_by_severity(client: TestClient, unique: str) -> None:
    """``?service=&severity=`` narrows to the intersection and ``total`` matches."""
    svc = f"sevsvc-{unique}"
    for sev in ("high", "high", "low"):
        client.post("/incidents", json=_payload(service=svc, severity=sev))

    resp = client.get(f"/incidents?service={svc}&severity=high")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 2
    assert len(data["items"]) == 2
    assert all(item["severity"] == "high" for item in data["items"])


def test_list_total_exceeds_page_when_limited(
    client: TestClient, unique: str
) -> None:
    """``total`` reflects all matches even when ``limit`` returns fewer items."""
    svc = f"pagesvc-{unique}"
    for i in range(5):
        client.post("/incidents", json=_payload(service=svc, title=f"P-{i}-{unique}"))

    resp = client.get(f"/incidents?service={svc}&limit=2")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 5  # full match count, not the page size
    assert len(data["items"]) == 2
    assert data["limit"] == 2


@pytest.mark.parametrize("bad_limit", [0, 201, -1])
def test_list_rejects_out_of_range_limit(
    client: TestClient, bad_limit: int
) -> None:
    """``limit`` outside 1–200 → 422."""
    resp = client.get(f"/incidents?limit={bad_limit}")
    assert resp.status_code == 422, resp.text
