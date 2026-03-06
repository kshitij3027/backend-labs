import pytest
import httpx
from app.node_server import create_node_app


@pytest.fixture
def node_app():
    return create_node_app("test-node")


@pytest.fixture
async def client(node_app):
    transport = httpx.ASGITransport(app=node_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def make_entry(key="k1", value="v1"):
    return {
        "key": key,
        "value": value,
        "timestamp": 1000.0,
        "vector_clock": {},
        "node_id": "coordinator",
    }


class TestNodeServer:
    async def test_health_endpoint(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "test-node"
        assert data["is_healthy"] is True
        assert data["keys_count"] == 0

    async def test_store_write_and_read(self, client):
        resp = await client.post("/store", json=make_entry("k1", "v1"))
        assert resp.status_code == 200
        assert resp.json()["success"] is True

        resp = await client.get("/store/k1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["key"] == "k1"
        assert data["value"] == "v1"

    async def test_store_list_keys(self, client):
        await client.post("/store", json=make_entry("a", "1"))
        await client.post("/store", json=make_entry("b", "2"))
        await client.post("/store", json=make_entry("c", "3"))

        resp = await client.get("/store")
        assert resp.status_code == 200
        keys = resp.json()["keys"]
        assert set(keys) == {"a", "b", "c"}

    async def test_fail_rejects_store(self, client):
        await client.post("/admin/fail")
        resp = await client.post("/store", json=make_entry())
        assert resp.status_code == 503

    async def test_fail_health_still_responds(self, client):
        await client.post("/admin/fail")
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["is_healthy"] is False

    async def test_recover_accepts_store(self, client):
        await client.post("/admin/fail")
        resp = await client.post("/store", json=make_entry())
        assert resp.status_code == 503

        await client.post("/admin/recover")
        resp = await client.post("/store", json=make_entry())
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    async def test_admin_data(self, client):
        await client.post("/store", json=make_entry("x", "val"))
        resp = await client.get("/admin/data")
        assert resp.status_code == 200
        data = resp.json()
        assert "x" in data
        assert data["x"]["value"] == "val"

    async def test_vector_clock_increments_on_write(self, client):
        await client.post("/store", json=make_entry("k1", "v1"))
        resp = await client.get("/store/k1")
        vc = resp.json()["vector_clock"]
        assert vc.get("test-node", 0) >= 1

        await client.post("/store", json=make_entry("k2", "v2"))
        resp = await client.get("/store/k2")
        vc2 = resp.json()["vector_clock"]
        assert vc2.get("test-node", 0) >= 2
