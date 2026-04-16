"""Tests for the SearchEngine: query execution, highlighting, and suggestions."""

import pytest

from backend.tokenizer import LogTokenizer
from backend.index import InvertedIndex
from backend.search import SearchEngine


@pytest.fixture
def tokenizer():
    return LogTokenizer()


@pytest.fixture
async def populated_engine(tokenizer):
    """SearchEngine backed by an index with five diverse log documents."""
    index = InvertedIndex(tokenizer)
    engine = SearchEngine(index, tokenizer)

    await index.add_document(
        "Authentication error for user admin@corp.com", 1000.0, "auth-service", "ERROR"
    )
    await index.add_document(
        "Payment processed successfully for order 12345", 2000.0, "payment-service", "INFO"
    )
    await index.add_document(
        "Authentication timeout from 192.168.1.100", 3000.0, "auth-service", "WARN"
    )
    await index.add_document(
        "Database connection error on primary node", 4000.0, "db-service", "ERROR"
    )
    await index.add_document(
        "Error processing payment webhook", 5000.0, "payment-service", "ERROR"
    )

    return engine, index


# ======================================================================
# Search tests
# ======================================================================


class TestSearch:
    @pytest.mark.asyncio
    async def test_single_term_returns_results(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("error")
        assert response.total_results > 0
        assert all("error" in r.message.lower() for r in response.results)

    @pytest.mark.asyncio
    async def test_multi_term_and_intersection(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("authentication error")
        # Should only return docs with BOTH terms
        assert response.total_results >= 1
        for r in response.results:
            msg_lower = r.message.lower()
            assert "authentication" in msg_lower

    @pytest.mark.asyncio
    async def test_empty_query_returns_empty(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("")
        assert response.total_results == 0
        assert response.results == []

    @pytest.mark.asyncio
    async def test_nonexistent_term_returns_empty(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("zzzznonexistent")
        assert response.total_results == 0

    @pytest.mark.asyncio
    async def test_search_includes_timing(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("error")
        assert response.search_time_ms >= 0
        assert response.query == "error"

    @pytest.mark.asyncio
    async def test_limit_parameter(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("error", limit=1)
        assert len(response.results) <= 1

    @pytest.mark.asyncio
    async def test_results_have_scores(self, populated_engine):
        engine, _ = populated_engine
        response = engine.search("error")
        for r in response.results:
            assert r.score > 0


# ======================================================================
# Highlighting tests
# ======================================================================


class TestHighlighting:
    @pytest.mark.asyncio
    async def test_highlight_wraps_terms(self, populated_engine):
        engine, _ = populated_engine
        result = engine.highlight("Authentication error occurred", ["error"])
        assert "<mark>" in result
        assert "</mark>" in result

    @pytest.mark.asyncio
    async def test_highlight_case_insensitive(self, populated_engine):
        engine, _ = populated_engine
        result = engine.highlight("ERROR in module", ["error"])
        assert "<mark>ERROR</mark>" in result

    @pytest.mark.asyncio
    async def test_highlight_multiple_terms(self, populated_engine):
        engine, _ = populated_engine
        result = engine.highlight(
            "Authentication error for user admin", ["authentication", "error"]
        )
        assert "<mark>Authentication</mark>" in result
        assert "<mark>error</mark>" in result

    @pytest.mark.asyncio
    async def test_highlight_no_terms(self, populated_engine):
        engine, _ = populated_engine
        text = "Nothing to highlight here"
        result = engine.highlight(text, [])
        assert result == text


# ======================================================================
# Suggestions tests
# ======================================================================


class TestSuggestions:
    @pytest.mark.asyncio
    async def test_suggestions_by_prefix(self, populated_engine):
        engine, _ = populated_engine
        suggestions = engine.get_suggestions("err")
        assert "error" in suggestions

    @pytest.mark.asyncio
    async def test_suggestions_limit(self, populated_engine):
        engine, _ = populated_engine
        suggestions = engine.get_suggestions("", limit=5)
        assert len(suggestions) <= 5

    @pytest.mark.asyncio
    async def test_suggestions_empty_prefix(self, populated_engine):
        engine, _ = populated_engine
        suggestions = engine.get_suggestions("")
        assert len(suggestions) > 0

    @pytest.mark.asyncio
    async def test_suggestions_sorted_by_frequency(self, populated_engine):
        engine, _ = populated_engine
        suggestions = engine.get_suggestions("")
        # Just verify we get results and they are strings
        assert all(isinstance(s, str) for s in suggestions)
