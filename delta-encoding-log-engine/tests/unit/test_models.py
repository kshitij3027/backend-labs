"""Unit tests for :mod:`app.models` (the generator API surface).

Covers:

* :data:`LogEntry` being a plain ``dict`` alias (not a Pydantic model).
* :class:`GenerateRequest` documented defaults and ``Field`` range guards
  (``count``, ``churn``, ``schema_width`` bounds; ``None`` knobs accepted).
* :class:`GenerateResponse` carrying an open list of dict entries and surviving
  a ``model_dump()`` round-trip with the entries' varying schemas intact.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models import GenerateRequest, GenerateResponse, LogEntry


# --------------------------------------------------------------------------- #
# LogEntry alias
# --------------------------------------------------------------------------- #
def test_logentry_is_a_plain_dict_alias():
    """``LogEntry`` is the built-in ``dict`` type, not a BaseModel subclass."""
    # ``dict[str, Any]`` has ``dict`` as its origin / usable as a constructor.
    entry: LogEntry = {"ts": 1, "level": "INFO"}
    assert isinstance(entry, dict)
    assert getattr(LogEntry, "__origin__", LogEntry) is dict


# --------------------------------------------------------------------------- #
# GenerateRequest defaults
# --------------------------------------------------------------------------- #
def test_generate_request_defaults():
    """An empty request carries the documented defaults."""
    req = GenerateRequest()

    assert req.count == 50
    assert req.seed is None
    assert req.churn is None
    assert req.schema_width is None


def test_generate_request_accepts_explicit_values():
    """All knobs round-trip when supplied within range."""
    req = GenerateRequest(count=200, seed=7, churn=0.25, schema_width=10)

    assert req.count == 200
    assert req.seed == 7
    assert req.churn == 0.25
    assert req.schema_width == 10


def test_generate_request_none_knobs_allowed():
    """``churn``/``schema_width``/``seed`` may be explicitly ``None``."""
    req = GenerateRequest(count=10, seed=None, churn=None, schema_width=None)

    assert req.churn is None
    assert req.schema_width is None
    assert req.seed is None


# --------------------------------------------------------------------------- #
# GenerateRequest validation guards
# --------------------------------------------------------------------------- #
def test_count_zero_raises():
    """``count`` must be >= 1 (``Field(ge=1)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(count=0)


def test_count_above_max_raises():
    """``count`` must be <= 100000 (``Field(le=100000)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(count=100001)


def test_churn_above_one_raises():
    """``churn`` must be <= 1.0 (``Field(le=1.0)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(churn=1.5)


def test_churn_below_zero_raises():
    """``churn`` must be >= 0.0 (``Field(ge=0.0)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(churn=-0.1)


def test_schema_width_zero_raises():
    """``schema_width`` must be >= 1 (``Field(ge=1)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(schema_width=0)


def test_schema_width_above_max_raises():
    """``schema_width`` must be <= 40 (``Field(le=40)``)."""
    with pytest.raises(ValidationError):
        GenerateRequest(schema_width=41)


# --------------------------------------------------------------------------- #
# GenerateResponse round-trip
# --------------------------------------------------------------------------- #
def test_generate_response_round_trips_via_model_dump():
    """``GenerateResponse`` preserves heterogeneous entries through model_dump."""
    logs = [
        {"ts": 1700000000000, "level": "INFO", "service": "auth-api"},
        {"ts": 1700000000050, "level": "ERROR", "error": "ETIMEDOUT"},
    ]
    resp = GenerateResponse(logs=logs, count=len(logs))

    dumped = resp.model_dump()

    assert dumped["count"] == 2
    # The varying-schema entries survive verbatim (open dicts, not coerced).
    assert dumped["logs"] == logs
    # The ERROR row keeps its extra field; the INFO row keeps none.
    assert "error" in dumped["logs"][1]
    assert "error" not in dumped["logs"][0]


def test_generate_response_count_independent_of_len():
    """``count`` is a stored scalar (not auto-derived) — mirrors len by convention."""
    logs: list[LogEntry] = [{"ts": 1}, {"ts": 2}, {"ts": 3}]
    resp = GenerateResponse(logs=logs, count=len(logs))

    assert resp.count == 3
    assert len(resp.logs) == 3


def test_generate_response_empty_logs():
    """An empty batch is representable (count 0, no entries)."""
    resp = GenerateResponse(logs=[], count=0)

    assert resp.model_dump() == {"logs": [], "count": 0}
