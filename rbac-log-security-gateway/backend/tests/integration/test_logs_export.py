"""Integration tests for /api/logs/export."""
import pytest
from httpx import AsyncClient

from src.shared import audit_service


@pytest.fixture(autouse=True)
def _reset_audit():
    audit_service.clear()
    yield
    audit_service.clear()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_export_unknown_resource_returns_400(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/logs/export?resource=invalid.thing", headers=_auth(admin_token))
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_export_missing_token_returns_401(async_client: AsyncClient) -> None:
    r = await async_client.get("/api/logs/export?resource=application.auth")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_admin_can_export_application(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/logs/export?resource=application.auth", headers=_auth(admin_token))
    assert r.status_code == 200
    assert r.json()["masked"] is False


@pytest.mark.asyncio
async def test_admin_denied_export_business_financial(async_client: AsyncClient, admin_token: str) -> None:
    """The one symbolic deny in the admin role: !logs:export:business.financial."""
    r = await async_client.get("/api/logs/export?resource=business.financial", headers=_auth(admin_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_developer_export_application_ok(async_client: AsyncClient, dev_token: str) -> None:
    r = await async_client.get("/api/logs/export?resource=application.api", headers=_auth(dev_token))
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_developer_denied_system_export(async_client: AsyncClient, dev_token: str) -> None:
    r = await async_client.get("/api/logs/export?resource=system.kernel", headers=_auth(dev_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_analyst_export_metrics_only(async_client: AsyncClient, analyst_token: str) -> None:
    # Allowed
    r = await async_client.get("/api/logs/export?resource=business.metrics", headers=_auth(analyst_token))
    assert r.status_code == 200
    # Denied
    r = await async_client.get("/api/logs/export?resource=business.customer", headers=_auth(analyst_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_support_denied_all_exports(async_client: AsyncClient, support_token: str) -> None:
    for resource in ("application.auth", "application.api", "business.customer"):
        r = await async_client.get(f"/api/logs/export?resource={resource}", headers=_auth(support_token))
        assert r.status_code == 403, f"support unexpectedly got {r.status_code} for {resource}"
