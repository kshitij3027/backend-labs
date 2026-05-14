"""Integration tests for AuditMiddleware — confirms every request is recorded."""
import pytest
from httpx import AsyncClient

from src.shared import audit_service


@pytest.fixture(autouse=True)
def _reset_audit() -> None:
    """Wipe in-memory audit log before each test."""
    audit_service.clear()
    yield
    audit_service.clear()


@pytest.mark.asyncio
async def test_health_request_is_recorded(async_client: AsyncClient) -> None:
    r = await async_client.get("/health")
    assert r.status_code == 200
    entries = audit_service.query(limit=10)
    assert len(entries) == 1
    assert entries[0].path == "/health"
    assert entries[0].method == "GET"
    assert entries[0].status == 200


@pytest.mark.asyncio
async def test_anonymous_request_has_no_username(async_client: AsyncClient) -> None:
    await async_client.get("/health")
    entry = audit_service.query(limit=1)[0]
    assert entry.username is None
    assert entry.user_id is None


@pytest.mark.asyncio
async def test_authenticated_request_records_username(async_client: AsyncClient, admin_token: str) -> None:
    await async_client.get("/api/auth/profile", headers={"Authorization": f"Bearer {admin_token}"})
    entry = audit_service.query(limit=1)[0]
    assert entry.username == "alice"
    assert entry.user_id is not None


@pytest.mark.asyncio
async def test_failed_login_creates_security_event(async_client: AsyncClient) -> None:
    r = await async_client.post("/api/auth/login", json={"username": "alice", "password": "wrong"})
    assert r.status_code == 401
    events = audit_service.security_events()
    assert len(events) == 1
    assert events[0].event_type == "auth_failure"
    assert events[0].status == 401


@pytest.mark.asyncio
async def test_missing_token_on_protected_endpoint_creates_security_event(async_client: AsyncClient) -> None:
    r = await async_client.get("/api/auth/profile")
    assert r.status_code == 401
    events = audit_service.security_events()
    assert len(events) == 1
    assert events[0].event_type == "auth_failure"


@pytest.mark.asyncio
async def test_summary_reflects_actual_traffic(async_client: AsyncClient, admin_token: str) -> None:
    await async_client.get("/health")
    await async_client.get("/api/auth/profile", headers={"Authorization": f"Bearer {admin_token}"})
    await async_client.get("/api/auth/profile")  # 401
    summary = audit_service.summary()
    assert summary["total_entries"] == 3
    assert summary["security_events"] == 1
    assert summary["by_user"].get("alice", 0) == 1
