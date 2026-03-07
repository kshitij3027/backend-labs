import json
import pytest
from src.storage.server import app, store


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        store._data.clear()  # Reset store between tests
        yield client


class TestNodeAPI:
    def test_health(self, client):
        """GET /health returns 200 and includes node_id."""
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert "node_id" in data

    def test_put_and_get_data(self, client):
        """PUT /data/mykey then GET /data/mykey returns the stored value."""
        resp = client.put("/data/mykey", json={"value": "hello"})
        assert resp.status_code == 200
        put_data = resp.get_json()
        assert put_data["value"] == "hello"
        assert put_data["version"] == 1

        resp = client.get("/data/mykey")
        assert resp.status_code == 200
        get_data = resp.get_json()
        assert get_data["key"] == "mykey"
        assert get_data["value"] == "hello"
        assert get_data["version"] == 1

    def test_get_nonexistent(self, client):
        """GET /data/nope returns 404."""
        resp = client.get("/data/nope")
        assert resp.status_code == 404

    def test_get_all_data(self, client):
        """PUT two keys, GET /data returns both."""
        client.put("/data/k1", json={"value": "v1"})
        client.put("/data/k2", json={"value": "v2"})
        resp = client.get("/data")
        assert resp.status_code == 200
        data = resp.get_json()
        entries = data["entries"]
        assert len(entries) == 2
        assert entries["k1"]["value"] == "v1"
        assert entries["k2"]["value"] == "v2"

    def test_get_keys(self, client):
        """PUT two keys, GET /keys returns both."""
        client.put("/data/k1", json={"value": "v1"})
        client.put("/data/k2", json={"value": "v2"})
        resp = client.get("/keys")
        assert resp.status_code == 200
        data = resp.get_json()
        assert set(data["keys"]) == {"k1", "k2"}

    def test_merkle_root(self, client):
        """GET /merkle/root returns a root_hash."""
        resp = client.get("/merkle/root")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "root_hash" in data
        assert len(data["root_hash"]) == 64

    def test_merkle_leaves(self, client):
        """PUT two keys, GET /merkle/leaves returns 2 entries."""
        client.put("/data/k1", json={"value": "v1"})
        client.put("/data/k2", json={"value": "v2"})
        resp = client.get("/merkle/leaves")
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data["leaves"]) == 2
        assert "k1" in data["leaves"]
        assert "k2" in data["leaves"]

    def test_put_increments_version(self, client):
        """PUT same key twice, version increments from 1 to 2."""
        resp1 = client.put("/data/key1", json={"value": "first"})
        assert resp1.get_json()["version"] == 1
        resp2 = client.put("/data/key1", json={"value": "second"})
        assert resp2.get_json()["version"] == 2
        assert resp2.get_json()["value"] == "second"
