"""HTTP route tests for ``POST /api/carts/{cart_id}``.

The cart endpoint sits on top of two pieces verified elsewhere:

* :class:`src.region_ring.RegionRing` — covered by
  ``tests/test_region_ring.py``.
* :class:`src.replication_controller.ReplicationController.write` —
  covered by ``tests/test_replication_controller.py`` and the
  fan-out side-effect test in ``tests/test_http_server.py``.

So this file's job is to verify the *integration* of those two pieces
through FastAPI: the route stamps the right home region, the payload
fans out to every region, and the ``region_hint`` query param overrides
the ring as documented.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config import AppConfig
from src.http_server import create_app


def _build_app() -> tuple[FastAPI, TestClient]:
    """Fresh app + TestClient with the canonical 3-region config."""
    config = AppConfig.from_env(
        env={
            "REGIONS": "us-east,europe,asia",
            "PRIMARY_PREFERENCE": "us-east,europe,asia",
        }
    )
    app = create_app(config)
    return app, TestClient(app)


@pytest.fixture
def app_and_client() -> tuple[FastAPI, TestClient]:
    """Yield (app, client) with lifespan fired via ``with`` block."""
    app, c = _build_app()
    with c as opened:
        yield app, opened


@pytest.fixture
def client(app_and_client: tuple[FastAPI, TestClient]) -> TestClient:
    return app_and_client[1]


def test_post_cart_returns_home_region_and_log_id(client: TestClient) -> None:
    """A successful cart write returns log_id, cart_id, home_region, vc."""
    res = client.post(
        "/api/carts/cart-001",
        json={
            "items": [{"sku": "SKU-1", "qty": 2}],
            "user": "alice",
        },
    )
    assert res.status_code == 200
    body = res.json()

    assert isinstance(body["log_id"], str) and body["log_id"]
    assert body["cart_id"] == "cart-001"
    # Home region must be one of the configured ones.
    assert body["home_region"] in {"us-east", "europe", "asia"}
    # First write at us-east advances its slot to 1.
    assert body["vector_clock"] == {"us-east": 1}
    assert body["logical_ts"] == 1


def test_post_cart_replicates_to_secondaries(
    app_and_client: tuple[FastAPI, TestClient],
) -> None:
    """A single cart POST lands the same payload in every region's store."""
    app, client = app_and_client
    res = client.post(
        "/api/carts/cart-002",
        json={
            "items": [{"sku": "SKU-9", "qty": 1}],
            "user": "bob",
        },
    )
    assert res.status_code == 200
    log_id = res.json()["log_id"]

    regions = app.state.regions
    for rid in ("us-east", "europe", "asia"):
        assert log_id in regions[rid].log_store, f"missing from {rid}"
        entry = regions[rid].log_store[log_id]
        # The payload round-trips intact (with the home_region added).
        assert entry.data["cart_id"] == "cart-002"
        assert entry.data["user"] == "bob"
        assert entry.data["items"] == [{"sku": "SKU-9", "qty": 1}]
        assert entry.data["home_region"] in {"us-east", "europe", "asia"}


def test_post_cart_with_region_hint_overrides_ring(client: TestClient) -> None:
    """``?region_hint=europe`` forces home_region=europe regardless of the ring."""
    res = client.post(
        "/api/carts/cart-001?region_hint=europe",
        json={
            "items": [{"sku": "SKU-1", "qty": 2}],
            "user": "alice",
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["home_region"] == "europe"


def test_post_cart_invalid_region_hint_falls_back_to_ring(client: TestClient) -> None:
    """An unknown region_hint value is ignored and the ring picks the home."""
    res = client.post(
        "/api/carts/cart-003?region_hint=mars",
        json={
            "items": [{"sku": "SKU-7", "qty": 1}],
            "user": "carol",
        },
    )
    assert res.status_code == 200
    body = res.json()
    # Falls back to the deterministic ring pick — must be a real region.
    assert body["home_region"] in {"us-east", "europe", "asia"}
