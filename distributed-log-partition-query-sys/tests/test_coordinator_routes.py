import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone

from httpx import AsyncClient, ASGITransport

from src.config import CoordinatorConfig
from src.coordinator.app import create_coordinator_app
from src.coordinator.cache import QueryCache
from src.coordinator.merger import ResultMerger
from src.coordinator.partition_map import PartitionMap
from src.coordinator.scatter_gather import ScatterGather, ScatterResult
from src.models import LogEntry, PartitionInfo


@pytest.fixture
def coordinator_app():
    config = CoordinatorConfig(
        partition_urls=["http://partition-1:8081", "http://partition-2:8082"],
    )
    app = create_coordinator_app(config)

    # Manually set up state since lifespan won't run with ASGITransport
    partition_map = PartitionMap()
    partition_map.register(
        PartitionInfo(partition_id="partition_1", url="http://partition-1:8081")
    )
    partition_map.register(
        PartitionInfo(partition_id="partition_2", url="http://partition-2:8082")
    )

    mock_client = AsyncMock()
    scatter_gather = ScatterGather(client=mock_client, timeout=5.0)
    merger = ResultMerger()
    cache = QueryCache(max_size=100)

    app.state.client = mock_client
    app.state.partition_map = partition_map
    app.state.scatter_gather = scatter_gather
    app.state.merger = merger
    app.state.cache = cache
    app.state.config = config

    return app


@pytest.fixture
async def client(coordinator_app):
    transport = ASGITransport(app=coordinator_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def make_entries(partition_id: str, count: int = 3) -> list[LogEntry]:
    now = datetime.now(tz=timezone.utc)
    return [
        LogEntry(
            timestamp=now,
            level="INFO",
            service="test-service",
            message=f"msg-{i}",
            partition_id=partition_id,
        )
        for i in range(count)
    ]


class TestCoordinatorRoutes:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["role"] == "coordinator"
        assert data["partitions"]["total"] == 2

    @pytest.mark.asyncio
    async def test_stats(self, client):
        resp = await client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["partitions"]["total"] == 2
        assert "cache" in data

    @pytest.mark.asyncio
    async def test_query_basic(self, coordinator_app, client):
        # Mock scatter to return entries from both partitions
        entries_p1 = make_entries("partition_1", 3)
        entries_p2 = make_entries("partition_2", 2)

        async def mock_scatter(partitions, query):
            return [
                ScatterResult(
                    partition_id="partition_1",
                    success=True,
                    entries=entries_p1,
                    response_time_ms=5.0,
                ),
                ScatterResult(
                    partition_id="partition_2",
                    success=True,
                    entries=entries_p2,
                    response_time_ms=4.0,
                ),
            ]

        coordinator_app.state.scatter_gather.scatter = mock_scatter

        resp = await client.post("/query", json={"limit": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_results"] == 5
        assert data["partitions_queried"] == 2
        assert data["partitions_successful"] == 2
        assert data["cached"] is False

    @pytest.mark.asyncio
    async def test_query_cache_hit(self, coordinator_app, client):
        entries = make_entries("partition_1", 2)

        async def mock_scatter(partitions, query):
            return [
                ScatterResult(
                    partition_id="partition_1",
                    success=True,
                    entries=entries,
                    response_time_ms=5.0,
                ),
                ScatterResult(
                    partition_id="partition_2",
                    success=True,
                    entries=[],
                    response_time_ms=3.0,
                ),
            ]

        coordinator_app.state.scatter_gather.scatter = mock_scatter

        # First request
        await client.post("/query", json={"limit": 10})
        # Second request (same query -- should be cached)
        resp = await client.post("/query", json={"limit": 10})
        data = resp.json()
        assert data["cached"] is True

    @pytest.mark.asyncio
    async def test_query_partial_failure(self, coordinator_app, client):
        entries = make_entries("partition_1", 3)

        async def mock_scatter(partitions, query):
            return [
                ScatterResult(
                    partition_id="partition_1",
                    success=True,
                    entries=entries,
                    response_time_ms=5.0,
                ),
                ScatterResult(
                    partition_id="partition_2",
                    success=False,
                    error="Timeout",
                    response_time_ms=5000.0,
                ),
            ]

        coordinator_app.state.scatter_gather.scatter = mock_scatter

        resp = await client.post("/query", json={"limit": 10})
        data = resp.json()
        assert data["partitions_queried"] == 2
        assert data["partitions_successful"] == 1
        assert data["total_results"] == 3
