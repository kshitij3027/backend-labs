"""Integration tests for the /v1/reports/{framework} dispatcher."""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "reports-api-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    from src.stats.counters import reset_counters_for_tests
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    yield
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client


@pytest.mark.asyncio
async def test_gdpr_report_endpoint_returns_bundle(app_and_client):
    _app, client = app_and_client
    r = await client.get(
        "/v1/reports/gdpr?from=2024-01-01T00:00:00%2B00:00&to=2030-01-01T00:00:00%2B00:00"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["framework"] == "gdpr"
    assert "records" in body
    assert "verify_result" in body
    assert "attestation_signature" in body
    assert body["extras"]["regulation_reference"].startswith("GDPR")


@pytest.mark.asyncio
async def test_hipaa_report_endpoint_returns_bundle(app_and_client):
    _app, client = app_and_client
    r = await client.get(
        "/v1/reports/hipaa?from=2024-01-01T00:00:00%2B00:00&to=2030-01-01T00:00:00%2B00:00"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["framework"] == "hipaa"
    assert body["extras"]["regulation_reference"].startswith("HIPAA")


@pytest.mark.asyncio
async def test_unknown_framework_returns_400(app_and_client):
    _app, client = app_and_client
    r = await client.get("/v1/reports/unknown")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_default_time_range_used_when_no_query(app_and_client):
    """Without from/to, the endpoint should default to last 30 days."""
    _app, client = app_and_client
    r = await client.get("/v1/reports/gdpr")
    assert r.status_code == 200
    body = r.json()
    # Time range should be non-empty strings.
    assert len(body["time_range"]) == 2
    assert all(isinstance(x, str) and len(x) > 10 for x in body["time_range"])


@pytest.mark.asyncio
async def test_soc2_report_endpoint(app_and_client):
    _app, client = app_and_client
    r = await client.get(
        "/v1/reports/soc2?from=2024-01-01T00:00:00%2B00:00&to=2030-01-01T00:00:00%2B00:00"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["framework"] == "soc2"
    assert "anomaly_indicators" in body["extras"]
    assert "SOC 2" in body["extras"]["regulation_reference"]


@pytest.mark.asyncio
async def test_pci_dss_report_endpoint(app_and_client):
    _app, client = app_and_client
    r = await client.get(
        "/v1/reports/pci_dss?from=2024-01-01T00:00:00%2B00:00&to=2030-01-01T00:00:00%2B00:00"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["framework"] == "pci_dss"
    assert "PCI DSS" in body["extras"]["regulation_reference"]
