"""Integration tests for /api/logs/search across all 4 demo roles."""
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
async def test_unknown_resource_returns_400(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=foo.bar", headers=_auth(admin_token))
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_missing_token_returns_401(async_client: AsyncClient) -> None:
    r = await async_client.get("/api/logs/search?resource=application.auth")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_admin_can_read_application_auth(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=application.auth", headers=_auth(admin_token))
    assert r.status_code == 200
    body = r.json()
    assert body["resource"] == "application.auth"
    assert body["masked"] is False
    assert body["aggregated"] is None
    assert body["count"] >= 5
    assert body["rbac_rule"] == "logs:read:*"


@pytest.mark.asyncio
async def test_admin_can_read_business_financial(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=business.financial", headers=_auth(admin_token))
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_developer_denied_business(async_client: AsyncClient, dev_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=business.metrics", headers=_auth(dev_token))
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert detail["error"] == "forbidden"
    assert "deny" in detail["reason"]


@pytest.mark.asyncio
async def test_developer_can_read_application(async_client: AsyncClient, dev_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=application.worker", headers=_auth(dev_token))
    assert r.status_code == 200
    assert r.json()["masked"] is False


@pytest.mark.asyncio
async def test_analyst_business_metrics_is_aggregated(async_client: AsyncClient, analyst_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=business.metrics", headers=_auth(analyst_token))
    assert r.status_code == 200
    body = r.json()
    assert body["aggregated"] is not None
    assert body["records"] is None
    assert body["count"] >= 1
    assert "by_level" in body["aggregated"]


@pytest.mark.asyncio
async def test_analyst_denied_business_customer(async_client: AsyncClient, analyst_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=business.customer", headers=_auth(analyst_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_support_business_customer_is_masked(async_client: AsyncClient, support_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=business.customer", headers=_auth(support_token))
    assert r.status_code == 200
    body = r.json()
    assert body["masked"] is True
    # PII keys must be replaced with ***
    for rec in body["records"]:
        for k in ("email", "ip", "phone", "user_id", "username"):
            if k in rec["fields"]:
                assert rec["fields"][k] == "***", f"{k} not masked"


@pytest.mark.asyncio
async def test_support_denied_system(async_client: AsyncClient, support_token: str) -> None:
    r = await async_client.get("/api/logs/search?resource=system.kernel", headers=_auth(support_token))
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_decision_is_recorded_in_audit_entry(async_client: AsyncClient, admin_token: str) -> None:
    """Confirms the route sets request.state.decision so middleware captures it."""
    await async_client.get("/api/logs/search?resource=application.auth", headers=_auth(admin_token))
    entries = audit_service.query(limit=5)
    search_entries = [e for e in entries if e.path.startswith("/api/logs/search")]
    assert len(search_entries) >= 1
    e = search_entries[0]
    assert e.decision == "allow"
    assert e.rule == "logs:read:*"
    assert e.username == "alice"
