import pytest
from httpx import AsyncClient, ASGITransport
from src.config import PartitionConfig
from src.partition.app import create_partition_app
from src.partition.data_generator import generate_sample_logs
from src.partition.storage import LogStorage
from src.partition.search import LogSearchEngine


@pytest.fixture
def partition_app():
    config = PartitionConfig(partition_id="test_partition", log_count=200, days_back=7)
    app = create_partition_app(config)
    # Manually initialize app state since ASGITransport doesn't trigger lifespan
    storage = LogStorage()
    entries = generate_sample_logs(config.log_count, config.days_back, config.partition_id)
    storage.load(entries)
    app.state.storage = storage
    app.state.search_engine = LogSearchEngine()
    return app


@pytest.fixture
async def client(partition_app):
    transport = ASGITransport(app=partition_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestPartitionRoutes:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["partition_id"] == "test_partition"
        assert data["log_count"] == 200

    @pytest.mark.asyncio
    async def test_query_basic(self, client):
        resp = await client.post("/query", json={"limit": 5})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_results"] == 5
        assert len(data["results"]) == 5
        assert data["partitions_queried"] == 1
        assert data["partitions_successful"] == 1

    @pytest.mark.asyncio
    async def test_query_with_filter(self, client):
        resp = await client.post("/query", json={
            "filters": [{"field": "level", "operator": "eq", "value": "ERROR"}],
            "limit": 50,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert all(r["level"] == "ERROR" for r in data["results"])

    @pytest.mark.asyncio
    async def test_query_no_limit(self, client):
        resp = await client.post("/query", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_results"] <= 200  # max is log_count
