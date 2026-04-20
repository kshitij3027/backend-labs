"""Tests for the pydantic models in ``src.models``.

The models are the shared contract between the indexer, the API, and
the WebSocket broadcaster, so breaking changes here ripple
everywhere. These tests lock in the validation rules that matter:
required fields, bounded numeric ranges, and the level whitelist.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    GenerateSampleRequest,
    LogEntry,
    SearchRequest,
)


def test_log_entry_valid() -> None:
    """A minimal LogEntry round-trips with expected defaults."""
    entry = LogEntry(doc_id=1, message="hello world", timestamp=1_700_000_000.0)

    assert entry.doc_id == 1
    assert entry.message == "hello world"
    assert entry.timestamp == pytest.approx(1_700_000_000.0)
    # Defaults: unknown service, INFO level, no stream id yet.
    assert entry.service == "unknown"
    assert entry.level == "INFO"
    assert entry.stream_id is None


def test_log_entry_rejects_unknown_level() -> None:
    """Arbitrary level strings are rejected by the Literal validator."""
    with pytest.raises(ValidationError):
        LogEntry(
            doc_id=1,
            message="hello",
            timestamp=1.0,
            level="NOTALEVEL",  # type: ignore[arg-type]
        )


def test_search_request_requires_q() -> None:
    """``q`` is mandatory and must be non-empty."""
    with pytest.raises(ValidationError):
        SearchRequest()  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        SearchRequest(q="")


def test_search_request_limit_bounds() -> None:
    """``limit`` must be in [1, 500]; 0 and 10_000 both raise."""
    with pytest.raises(ValidationError):
        SearchRequest(q="foo", limit=0)

    with pytest.raises(ValidationError):
        SearchRequest(q="foo", limit=10_000)

    # Sanity: boundary values are accepted.
    assert SearchRequest(q="foo", limit=1).limit == 1
    assert SearchRequest(q="foo", limit=500).limit == 500


def test_generate_sample_request_rejects_negative() -> None:
    """Negative counts cannot be coerced by the ``ge=1`` constraint."""
    with pytest.raises(ValidationError):
        GenerateSampleRequest(count=-1)

    # Default still works with no kwargs.
    assert GenerateSampleRequest().count == 500
