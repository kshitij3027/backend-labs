"""Tests for :mod:`src.models`.

Exercises the validation rules that matter to the HTTP surface: empty
queries get rejected, over-sized limits get rejected, defaults fire
when fields are omitted, and full responses round-trip losslessly via
JSON so the service can serialize and the client can deserialize.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    HealthResponse,
    LogEntry,
    RankingExplanation,
    SearchRequest,
    SearchResponse,
    SearchResult,
    StatsResponse,
)


# ---------------------------------------------------------------------------
# SearchRequest
# ---------------------------------------------------------------------------

def test_search_request_rejects_empty_query() -> None:
    """An empty string must fail the ``min_length=1`` check."""
    with pytest.raises(ValidationError):
        SearchRequest(query="")


def test_search_request_rejects_over_limit() -> None:
    """``limit`` greater than 500 must be rejected by the upper bound."""
    with pytest.raises(ValidationError):
        SearchRequest(query="x", limit=501)


def test_search_request_rejects_under_limit() -> None:
    """``limit`` below 1 must be rejected by the lower bound."""
    with pytest.raises(ValidationError):
        SearchRequest(query="x", limit=0)


def test_search_request_context_default_is_none() -> None:
    """When the client omits ``context`` the default must be None."""
    req = SearchRequest(query="x", limit=10)
    assert req.context is None


def test_search_request_limit_default_is_ten() -> None:
    """Omitted ``limit`` must default to 10 per the ranker contract."""
    req = SearchRequest(query="x")
    assert req.limit == 10


# ---------------------------------------------------------------------------
# LogEntry
# ---------------------------------------------------------------------------

def test_log_entry_defaults() -> None:
    """Minimal entry: service defaults to ``unknown`` and level to INFO."""
    entry = LogEntry(message="m", timestamp=1.0)
    assert entry.service == "unknown"
    assert entry.level == "INFO"
    assert entry.id is None
    assert entry.metadata == {}


def test_log_entry_rejects_empty_message() -> None:
    """Empty ``message`` must be rejected by ``min_length=1``."""
    with pytest.raises(ValidationError):
        LogEntry(message="", timestamp=0.0)


def test_log_entry_accepts_known_levels() -> None:
    """Every documented level must validate."""
    for level in ("DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"):
        entry = LogEntry(message="m", timestamp=1.0, level=level)
        assert entry.level == level


def test_log_entry_rejects_unknown_level() -> None:
    """Anything outside the Literal set must fail validation."""
    with pytest.raises(ValidationError):
        LogEntry(message="m", timestamp=1.0, level="NOTALEVEL")


# ---------------------------------------------------------------------------
# SearchResponse round-trip
# ---------------------------------------------------------------------------

def test_search_response_round_trip() -> None:
    """Build a full response, JSON-serialize, deserialize, assert equal."""
    explanation = RankingExplanation(
        tfidf=0.45,
        temporal=0.25,
        severity=0.15,
        service=0.10,
        context=0.05,
        reasons=["incident_mode_boost"],
    )
    result = SearchResult(
        log_entry="authentication failed for user=alice",
        timestamp=1700000000.0,
        service="auth",
        level="ERROR",
        score=0.99,
        ranking_explanation=explanation,
    )
    response = SearchResponse(
        query="authentication error",
        intent="troubleshooting",
        expanded_terms=["authentication", "error", "auth"],
        results=[result],
        total_hits=17,
        ranked_hits=1,
        execution_time_ms=42.5,
    )

    raw = response.model_dump_json()
    restored = SearchResponse.model_validate_json(raw)
    assert restored == response


# ---------------------------------------------------------------------------
# StatsResponse + HealthResponse
# ---------------------------------------------------------------------------

def test_stats_response_zero_values() -> None:
    """A freshly started app should be able to report zeros without crashing."""
    stats = StatsResponse(
        total_docs=0,
        unique_tokens=0,
        index_version=0,
        idf_version=0,
        cache_hit_ratio=0.0,
        p95_latency_ms=0.0,
    )
    assert stats.total_docs == 0
    assert stats.cache_hit_ratio == 0.0


def test_health_response_default_status() -> None:
    """The health response must default to exactly ``"ok"``."""
    assert HealthResponse().status == "ok"
