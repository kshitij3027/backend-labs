"""Unit tests for the C3 Pydantic request/response contracts (:mod:`src.schemas`).

Pure — no DB, no HTTP. These assert the *shape* of the incident wire contract:

* ``IncidentCreate`` validation: rejects blank required free-text, rejects a
  ``severity`` outside the canonical closed set, accepts a well-formed payload,
  and cleans ``tags`` (trim + dedupe, order preserved).
* ``IncidentCreate`` never carries an ``embedding`` field (vectors are C5).
* ``IncidentOut``/``from_orm_incident`` derives ``has_embedding`` from the raw
  vector and never leaks the vector itself.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.schemas import SEVERITIES, IncidentCreate, IncidentOut


def _valid_payload(**overrides: object) -> dict:
    """A minimal, valid ``IncidentCreate`` payload; override any field."""
    payload = {
        "title": "Database connection pool exhausted",
        "description": "Requests timed out waiting on a checked-out DB connection.",
        "service": "orders-api",
        "severity": "high",
        "tags": ["db", "timeout"],
        "resolution": "Raised max pool size and added a statement timeout.",
    }
    payload.update(overrides)
    return payload


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #
def test_valid_payload_accepted() -> None:
    """A well-formed payload validates and preserves its fields."""
    model = IncidentCreate(**_valid_payload())
    assert model.title == "Database connection pool exhausted"
    assert model.service == "orders-api"
    assert model.severity == "high"
    assert model.tags == ["db", "timeout"]
    assert model.resolution.startswith("Raised max pool size")


def test_all_canonical_severities_accepted() -> None:
    """Every canonical severity in :data:`SEVERITIES` is a valid input."""
    for sev in SEVERITIES:
        model = IncidentCreate(**_valid_payload(severity=sev))
        assert model.severity == sev


def test_no_embedding_field_on_create() -> None:
    """``IncidentCreate`` must not expose an ``embedding`` field (computed in C5)."""
    assert "embedding" not in IncidentCreate.model_fields
    # Extra keys are ignored by default, so an embedding passed in is dropped, not
    # stored — the model has no attribute for it.
    model = IncidentCreate(**_valid_payload(embedding=[0.1] * 384))
    assert not hasattr(model, "embedding")


# --------------------------------------------------------------------------- #
# Rejections — blank required free-text
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("field", ["title", "description", "resolution", "service"])
@pytest.mark.parametrize("bad", ["", "   ", "\t\n  "])
def test_blank_required_fields_rejected(field: str, bad: str) -> None:
    """Blank / whitespace-only required text raises ``ValidationError``."""
    with pytest.raises(ValidationError):
        IncidentCreate(**_valid_payload(**{field: bad}))


def test_missing_required_field_rejected() -> None:
    """Omitting a required field raises ``ValidationError``."""
    payload = _valid_payload()
    del payload["title"]
    with pytest.raises(ValidationError):
        IncidentCreate(**payload)


# --------------------------------------------------------------------------- #
# Rejections — severity outside the closed set
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad_severity", ["urgent", "sev1", "HIGH", "", "warning"])
def test_severity_outside_canonical_set_rejected(bad_severity: str) -> None:
    """A ``severity`` not in the canonical Literal set raises ``ValidationError``."""
    with pytest.raises(ValidationError):
        IncidentCreate(**_valid_payload(severity=bad_severity))


# --------------------------------------------------------------------------- #
# Tag cleaning: trim + dedupe, order preserved
# --------------------------------------------------------------------------- #
def test_tags_trimmed_and_deduped_preserving_order() -> None:
    """Tags are stripped, blanks dropped, and duplicates removed in first-seen order."""
    model = IncidentCreate(
        **_valid_payload(tags=["  db ", "db", "timeout", "", "  ", "timeout", "p1"])
    )
    assert model.tags == ["db", "timeout", "p1"]


def test_tags_default_empty() -> None:
    """Omitting ``tags`` yields an empty list, not ``None``."""
    payload = _valid_payload()
    del payload["tags"]
    model = IncidentCreate(**payload)
    assert model.tags == []


# --------------------------------------------------------------------------- #
# IncidentOut / from_orm_incident
# --------------------------------------------------------------------------- #
class _FakeIncident:
    """Stand-in ORM row for exercising ``from_orm_incident`` without a DB."""

    def __init__(self, embedding: list[float] | None) -> None:
        self.id = 7
        self.title = "t"
        self.description = "d"
        self.service = "svc"
        self.severity = "medium"
        self.tags = ["a", "b"]
        self.resolution = "r"
        self.created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.embedding = embedding


def test_incident_out_has_embedding_false_when_null() -> None:
    """A NULL embedding → ``has_embedding == False`` (the C3 state)."""
    out = IncidentOut.from_orm_incident(_FakeIncident(embedding=None))
    assert out.has_embedding is False
    assert out.id == 7
    assert out.severity == "medium"
    assert out.tags == ["a", "b"]


def test_incident_out_has_embedding_true_when_present() -> None:
    """A populated embedding → ``has_embedding == True`` (post-C5 state)."""
    out = IncidentOut.from_orm_incident(_FakeIncident(embedding=[0.0] * 384))
    assert out.has_embedding is True


def test_incident_out_never_serialises_the_vector() -> None:
    """The raw vector is never part of the serialised response."""
    out = IncidentOut.from_orm_incident(_FakeIncident(embedding=[0.5] * 384))
    dumped = out.model_dump()
    assert "embedding" not in dumped
    assert dumped["has_embedding"] is True
    assert "embedding" not in IncidentOut.model_fields
