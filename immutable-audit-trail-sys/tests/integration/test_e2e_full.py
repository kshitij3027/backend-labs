"""Codified end-to-end test that mirrors scripts/e2e.sh.

Runs against the app in-process via LifespanManager — independent of
the Docker compose stack, so it's part of `make test` as well.
"""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "e2e-full-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    from src.stats.counters import reset_counters_for_tests
    from src.anomaly.alerts import reset_sink_for_tests
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    reset_sink_for_tests()
    yield
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    reset_sink_for_tests()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client


def _digest(seed: int = 0) -> str:
    return ("%064x" % seed)[:64].ljust(64, "0")


@pytest.mark.asyncio
async def test_full_e2e_flow(app_and_client):
    """Cover every endpoint the e2e.sh script exercises."""
    _app, client = app_and_client

    # Health
    r = await client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"

    # 5 appends with headers
    for actor in ["alice", "bob", "carol", "dave", "eve"]:
        body = {
            "action": "read", "resource": "LOG_e2e", "success": True,
            "args_digest": _digest(hash(actor) & 0xFFFFFFFFFFFFFFFF),
            "result_digest": _digest((hash(actor) + 1) & 0xFFFFFFFFFFFFFFFF),
            "processing_ms": 1.0,
        }
        r = await client.post("/v1/audit/append", json=body, headers={"X-User-ID": actor})
        assert r.status_code == 201, r.text

    # Records query
    assert (await client.get("/v1/records")).status_code == 200
    assert (await client.get("/v1/records?actor=alice&limit=10")).status_code == 200
    assert (await client.get("/v1/records/0")).status_code == 200

    # head_seq should be 5 now
    verify = (await client.get("/v1/verify")).json()
    assert verify["ok"] is True
    assert verify["head_seq"] == 5
    assert verify["total_records"] == 6  # genesis + 5

    # Compliance reports
    for fw in ["gdpr", "hipaa", "soc2", "pci_dss"]:
        r = await client.get(f"/v1/reports/{fw}")
        assert r.status_code == 200, f"{fw} -> {r.status_code}"
    r = await client.get("/v1/reports/unknown_framework")
    assert r.status_code == 400

    # Observability
    stats = (await client.get("/api/stats")).json()
    assert "records_appended" in stats
    metrics = (await client.get("/metrics")).text
    assert "audit_records_appended_total" in metrics

    # Dashboard
    assert (await client.get("/")).status_code == 200
    assert (await client.get("/static/dashboard.css")).status_code == 200
    assert (await client.get("/static/htmx.min.js")).status_code == 200
    for partial in ["stats", "records", "integrity", "alerts"]:
        r = await client.get(f"/partials/{partial}")
        assert r.status_code == 200, f"/partials/{partial} -> {r.status_code}"
