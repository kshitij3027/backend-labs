"""Unit tests for the API-facing Pydantic models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    CacheMeta,
    InvalidateRequest,
    QueryRequest,
    QueryResponse,
)


def test_query_request_ok_with_default_params() -> None:
    req = QueryRequest(query="x")
    assert req.query == "x"
    assert req.params == {}


def test_query_request_requires_query() -> None:
    with pytest.raises(ValidationError):
        QueryRequest()  # type: ignore[call-arg]


def test_invalidate_request_requires_pattern_or_tags() -> None:
    with pytest.raises(ValidationError):
        InvalidateRequest()


def test_invalidate_request_ok_with_pattern() -> None:
    req = InvalidateRequest(pattern="q:*")
    assert req.pattern == "q:*"
    assert req.tags == []


def test_invalidate_request_ok_with_tags() -> None:
    req = InvalidateRequest(tags=["source:web"])
    assert req.tags == ["source:web"]


def test_query_response_round_trips_with_cache_meta() -> None:
    meta = CacheMeta(tier="l1", elapsed_ms=1.5, key="abc123")
    resp = QueryResponse(result={"count": 7}, meta=meta)

    dumped = resp.model_dump()
    restored = QueryResponse.model_validate(dumped)

    assert restored.result == {"count": 7}
    assert restored.meta.tier == "l1"
    assert restored.meta.elapsed_ms == 1.5
    assert restored.meta.key == "abc123"
    assert restored.meta.degraded is False
