"""Tests for the FastAPI API endpoints."""

import pytest


@pytest.mark.asyncio
async def test_health_endpoint(client):
    """GET /health returns 200 with healthy status and index counters."""
    response = await client.get("/health")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert "documents" in data
    assert "terms" in data


class TestSearchEndpoint:
    @pytest.mark.asyncio
    async def test_search_returns_results(self, client):
        response = await client.get("/api/search", params={"q": "error"})
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert "total_results" in data
        assert "search_time_ms" in data

    @pytest.mark.asyncio
    async def test_search_empty_query(self, client):
        response = await client.get("/api/search", params={"q": ""})
        assert response.status_code == 200
        data = response.json()
        assert data["total_results"] == 0


class TestStatsEndpoint:
    @pytest.mark.asyncio
    async def test_stats_returns_counts(self, client):
        response = await client.get("/api/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_documents"] >= 10  # sample data loaded at startup
        assert data["total_terms"] > 0


class TestIndexEndpoint:
    @pytest.mark.asyncio
    async def test_index_single_document(self, client):
        response = await client.post(
            "/api/index",
            json={
                "message": "Test log entry for indexing",
                "timestamp": 1700000000.0,
                "service": "test-service",
                "level": "INFO",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "doc_id" in data

    @pytest.mark.asyncio
    async def test_bulk_index(self, client):
        response = await client.post(
            "/api/index/bulk",
            json={
                "documents": [
                    {
                        "message": "Bulk test one",
                        "timestamp": 1.0,
                        "service": "svc",
                        "level": "INFO",
                    },
                    {
                        "message": "Bulk test two",
                        "timestamp": 2.0,
                        "service": "svc",
                        "level": "WARN",
                    },
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2


class TestSuggestionsEndpoint:
    @pytest.mark.asyncio
    async def test_suggestions_returns_list(self, client):
        response = await client.get(
            "/api/suggestions", params={"prefix": "err"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "suggestions" in data
