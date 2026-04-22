"""Tests for :class:`src.query.parser.QueryParser` and :class:`ParsedQuery`.

These tests exercise the full composition: tokenizer + intent
detector + synonym expander glued through ``QueryParser``. Fixtures
come from ``tests/conftest.py`` (``settings``).
"""

from __future__ import annotations

import pytest

from src.index.tokenizer import LogTokenizer
from src.query.intent import IntentDetector
from src.query.parser import ParsedQuery, QueryParser
from src.query.synonyms import SynonymExpander


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def parser(settings) -> QueryParser:
    """Build a parser wired with default dependencies."""
    return QueryParser(
        tokenizer=LogTokenizer(settings),
        intent=IntentDetector(),
        synonyms=SynonymExpander(),
    )


# ---------------------------------------------------------------------------
# End-to-end parse
# ---------------------------------------------------------------------------


def test_parse_authentication_error(parser: QueryParser):
    """A query hitting user_activity should produce tokens + expansions.

    Priority: ``authentication`` matches the user_activity bucket
    before ``error`` could fire the troubleshooting bucket, so the
    intent must be ``user_activity``. Token list contains both head
    words; expanded list includes at least one synonym from the
    default dict (``failure`` / ``exception`` / ``fault``).
    """
    parsed = parser.parse("authentication error")
    assert parsed.intent == "user_activity"
    assert "authentication" in parsed.tokens
    assert "error" in parsed.tokens
    # Originals must survive the expansion step.
    assert "authentication" in parsed.expanded_tokens
    assert "error" in parsed.expanded_tokens
    # ``error`` has three synonyms in the default dict — at least one fires.
    synonym_candidates = {"failure", "exception", "fault"}
    assert synonym_candidates.intersection(parsed.expanded_tokens)


def test_parse_payment_timeout_hits_payment_analysis(parser: QueryParser):
    """Payment keyword outranks the performance bucket's ``timeout``."""
    parsed = parser.parse("payment timeout")
    assert parsed.intent == "payment_analysis"


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------


def test_parse_empty_string(parser: QueryParser):
    """Empty query yields an empty token list and the fallback intent."""
    parsed = parser.parse("")
    assert parsed.intent == "general_search"
    assert parsed.tokens == []
    assert parsed.expanded_tokens == []
    assert parsed.raw == ""


# ---------------------------------------------------------------------------
# Dataclass shape
# ---------------------------------------------------------------------------


def test_parsed_query_dataclass_roundtrip():
    """Construct a ``ParsedQuery`` directly and access every field.

    The ``metadata`` field should default to an empty dict so downstream
    code can poke at it without a None check.
    """
    pq = ParsedQuery(
        raw="hi",
        tokens=["hi"],
        expanded_tokens=["hi", "hello"],
        intent="general_search",
    )
    assert pq.raw == "hi"
    assert pq.tokens == ["hi"]
    assert pq.expanded_tokens == ["hi", "hello"]
    assert pq.intent == "general_search"
    assert pq.metadata == {}
    # Ensure ``metadata`` is a fresh dict per instance (not a shared default).
    other = ParsedQuery(raw="", tokens=[], expanded_tokens=[], intent="general_search")
    pq.metadata["k"] = 1
    assert "k" not in other.metadata
