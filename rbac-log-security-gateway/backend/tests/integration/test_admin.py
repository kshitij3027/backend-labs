"""Integration tests for admin endpoints. Confirms admin-only enforcement."""
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


ENDPOINTS = [
    "/api/admin/audit-summary",
    "/api/admin/security-events",
    "/api/admin/audit-entries",
    "/api/admin/rbac-policies",
    "/api/admin/system-status",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ENDPOINTS)
async def test_admin_can_access(async_client: AsyncClient, admin_token: str, path: str) -> None:
    r = await async_client.get(path, headers=_auth(admin_token))
    assert r.status_code == 200, f"admin denied {path}: {r.text[:200]}"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ENDPOINTS)
async def test_developer_forbidden(async_client: AsyncClient, dev_token: str, path: str) -> None:
    r = await async_client.get(path, headers=_auth(dev_token))
    assert r.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ENDPOINTS)
async def test_analyst_forbidden(async_client: AsyncClient, analyst_token: str, path: str) -> None:
    r = await async_client.get(path, headers=_auth(analyst_token))
    assert r.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ENDPOINTS)
async def test_support_forbidden(async_client: AsyncClient, support_token: str, path: str) -> None:
    r = await async_client.get(path, headers=_auth(support_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_anon_forbidden(async_client: AsyncClient) -> None:
    for path in ENDPOINTS:
        r = await async_client.get(path)
        assert r.status_code == 401


@pytest.mark.asyncio
async def test_audit_summary_reflects_traffic(async_client: AsyncClient, admin_token: str) -> None:
    """Hit a couple endpoints, then confirm the summary picks them up."""
    await async_client.get("/health")
    await async_client.get("/api/auth/profile", headers=_auth(admin_token))
    r = await async_client.get("/api/admin/audit-summary", headers=_auth(admin_token))
    body = r.json()
    assert body["total_entries"] >= 2


@pytest.mark.asyncio
async def test_rbac_policies_lists_all_four_roles(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/admin/rbac-policies", headers=_auth(admin_token))
    body = r.json()
    assert set(body["roles"].keys()) == {"administrator", "developer", "analyst", "support"}
    assert set(body["default_scopes"].keys()) == {"administrator", "developer", "analyst", "support"}


@pytest.mark.asyncio
async def test_system_status_lists_known_resources(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/admin/system-status", headers=_auth(admin_token))
    body = r.json()
    assert body["status"] == "ok"
    assert "application.auth" in body["known_resources"]
    assert "system.audit" in body["known_resources"]
    assert len(body["known_resources"]) == 8


@pytest.mark.asyncio
async def test_security_events_endpoint_returns_list(async_client: AsyncClient, admin_token: str) -> None:
    # Trigger a security event first
    await async_client.post("/api/auth/login", json={"username": "alice", "password": "wrong"})
    r = await async_client.get("/api/admin/security-events", headers=_auth(admin_token))
    assert r.status_code == 200
    events = r.json()
    assert len(events) >= 1
    assert events[0]["event_type"] == "auth_failure"
