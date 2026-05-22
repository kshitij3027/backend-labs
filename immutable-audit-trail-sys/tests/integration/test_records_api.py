"""Integration tests for /v1/audit/append + /v1/records endpoints."""
import base64
import importlib
import os

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    monkeypatch.setenv("SIGNING_KEY_B64", base64.b64encode(os.urandom(32)).decode())
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'audit.db'}")
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "records-api-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    get_settings.cache_clear()
    clear_appender()
    yield
    get_settings.cache_clear()
    clear_appender()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client


def _append_body(**overrides) -> dict:
    base = dict(
        action="read",
        resource="LOG_/var/log/app",
        success=True,
        args_digest="a" * 64,
        result_digest="b" * 64,
        processing_ms=2.5,
    )
    base.update(overrides)
    return base


# --- POST /v1/audit/append ----------------------------------------------

@pytest.mark.asyncio
async def test_append_returns_201_with_sealed_record(app_and_client):
    _app, client = app_and_client
    r = await client.post("/v1/audit/append", json=_append_body())
    assert r.status_code == 201
    body = r.json()
    assert body["seq"] == 1  # genesis is 0, this is the first real append
    assert body["actor"] == "anonymous"  # no header, no body actor
    assert len(body["self_hash"]) == 64
    assert body["signature"]
    assert body["prev_hash"] != "0" * 64  # links to genesis self_hash


@pytest.mark.asyncio
async def test_append_uses_body_actor_when_provided(app_and_client):
    _app, client = app_and_client
    r = await client.post(
        "/v1/audit/append",
        json=_append_body(actor="alice"),
    )
    assert r.status_code == 201
    assert r.json()["actor"] == "alice"


@pytest.mark.asyncio
async def test_append_uses_header_actor_when_no_body_actor(app_and_client):
    _app, client = app_and_client
    r = await client.post(
        "/v1/audit/append",
        json=_append_body(),
        headers={"X-User-ID": "bob"},
    )
    assert r.status_code == 201
    assert r.json()["actor"] == "bob"


@pytest.mark.asyncio
async def test_body_actor_wins_over_header(app_and_client):
    _app, client = app_and_client
    r = await client.post(
        "/v1/audit/append",
        json=_append_body(actor="alice"),
        headers={"X-User-ID": "bob"},
    )
    assert r.status_code == 201
    assert r.json()["actor"] == "alice"


@pytest.mark.asyncio
async def test_append_failure_record_persists_error_message(app_and_client):
    _app, client = app_and_client
    r = await client.post(
        "/v1/audit/append",
        json=_append_body(success=False, error_message="permission denied", result_digest=""),
    )
    assert r.status_code == 201
    body = r.json()
    assert body["success"] is False
    assert body["error_message"] == "permission denied"


@pytest.mark.asyncio
async def test_append_validates_field_lengths(app_and_client):
    """args_digest must be exactly 64 chars; bad length -> 422."""
    _app, client = app_and_client
    r = await client.post(
        "/v1/audit/append",
        json=_append_body(args_digest="short"),
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_append_rejects_unknown_fields(app_and_client):
    _app, client = app_and_client
    payload = _append_body()
    payload["bogus_field"] = "x"
    r = await client.post("/v1/audit/append", json=payload)
    assert r.status_code == 422


# --- GET /v1/records ------------------------------------------------------

@pytest.mark.asyncio
async def test_list_records_returns_genesis_when_empty(app_and_client):
    _app, client = app_and_client
    r = await client.get("/v1/records")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1  # just genesis
    assert body["records"][0]["seq"] == 0
    assert body["records"][0]["actor"] == "system"


@pytest.mark.asyncio
async def test_list_records_sorted_desc_by_seq(app_and_client):
    _app, client = app_and_client
    for _ in range(5):
        await client.post("/v1/audit/append", json=_append_body())
    r = await client.get("/v1/records")
    seqs = [rec["seq"] for rec in r.json()["records"]]
    assert seqs == sorted(seqs, reverse=True)
    assert seqs[0] == 5  # newest first


@pytest.mark.asyncio
async def test_list_records_filter_by_actor(app_and_client):
    _app, client = app_and_client
    await client.post("/v1/audit/append", json=_append_body(actor="alice"))
    await client.post("/v1/audit/append", json=_append_body(actor="alice"))
    await client.post("/v1/audit/append", json=_append_body(actor="bob"))
    r = await client.get("/v1/records?actor=alice")
    body = r.json()
    assert body["count"] == 2
    assert all(rec["actor"] == "alice" for rec in body["records"])


@pytest.mark.asyncio
async def test_list_records_filter_by_action_and_resource(app_and_client):
    _app, client = app_and_client
    await client.post("/v1/audit/append", json=_append_body(action="search", resource="X"))
    await client.post("/v1/audit/append", json=_append_body(action="search", resource="Y"))
    await client.post("/v1/audit/append", json=_append_body(action="export", resource="X"))
    r = await client.get("/v1/records?action=search&resource=X")
    assert r.json()["count"] == 1


@pytest.mark.asyncio
async def test_list_records_limit_and_offset(app_and_client):
    _app, client = app_and_client
    for _ in range(10):
        await client.post("/v1/audit/append", json=_append_body())
    r = await client.get("/v1/records?limit=3&offset=2")
    body = r.json()
    assert body["count"] == 3
    assert body["limit"] == 3
    assert body["offset"] == 2


@pytest.mark.asyncio
async def test_list_records_rejects_bad_limit(app_and_client):
    _app, client = app_and_client
    r = await client.get("/v1/records?limit=0")
    assert r.status_code == 422
    r = await client.get("/v1/records?limit=501")
    assert r.status_code == 422


# --- GET /v1/records/{seq} -----------------------------------------------

@pytest.mark.asyncio
async def test_get_single_record_returns_genesis(app_and_client):
    _app, client = app_and_client
    r = await client.get("/v1/records/0")
    assert r.status_code == 200
    body = r.json()
    assert body["seq"] == 0
    assert body["actor"] == "system"
    assert body["action"] == "genesis"


@pytest.mark.asyncio
async def test_get_single_record_404_for_missing_seq(app_and_client):
    _app, client = app_and_client
    r = await client.get("/v1/records/9999")
    assert r.status_code == 404
    assert "9999" in r.json()["detail"]


# --- Chain integrity through the HTTP layer -----------------------------

@pytest.mark.asyncio
async def test_chain_holds_across_http_appends(app_and_client):
    """5 HTTP appends followed by a GET of each — verify prev_hash linkage."""
    app, client = app_and_client
    seqs = []
    for _ in range(5):
        r = await client.post("/v1/audit/append", json=_append_body())
        seqs.append(r.json()["seq"])
    assert seqs == [1, 2, 3, 4, 5]
    prev_hash = (await client.get("/v1/records/0")).json()["self_hash"]
    for s in seqs:
        rec = (await client.get(f"/v1/records/{s}")).json()
        assert rec["prev_hash"] == prev_hash, f"chain break at seq={s}"
        prev_hash = rec["self_hash"]

    # And the in-process verifier should agree.
    result = await app.state.chain_verifier.verify_full()
    assert result.ok is True
