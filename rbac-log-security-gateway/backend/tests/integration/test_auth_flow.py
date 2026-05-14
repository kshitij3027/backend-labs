"""Integration tests for /api/auth/login + /api/auth/profile."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_login_returns_token_and_user_info(async_client: AsyncClient) -> None:
    r = await async_client.post("/api/auth/login", json={"username": "alice", "password": "admin123"})
    assert r.status_code == 200
    body = r.json()
    assert "access_token" in body and len(body["access_token"]) > 20
    assert body["token_type"] == "bearer"
    assert body["user_info"]["username"] == "alice"
    assert body["user_info"]["roles"] == ["administrator"]


@pytest.mark.asyncio
async def test_login_rejects_bad_password(async_client: AsyncClient) -> None:
    r = await async_client.post("/api/auth/login", json={"username": "alice", "password": "wrong"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_login_rejects_unknown_user(async_client: AsyncClient) -> None:
    r = await async_client.post("/api/auth/login", json={"username": "eve", "password": "x"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_profile_requires_token(async_client: AsyncClient) -> None:
    r = await async_client.get("/api/auth/profile")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_profile_returns_user_info(async_client: AsyncClient, admin_token: str) -> None:
    r = await async_client.get("/api/auth/profile", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    body = r.json()
    assert body["username"] == "alice"
    assert "administrator" in body["roles"]


@pytest.mark.asyncio
async def test_profile_rejects_garbage_token(async_client: AsyncClient) -> None:
    r = await async_client.get("/api/auth/profile", headers={"Authorization": "Bearer garbage"})
    assert r.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("username,password,expected_role", [
    ("alice", "admin123", "administrator"),
    ("bob", "dev123", "developer"),
    ("carol", "analyst123", "analyst"),
    ("dave", "support123", "support"),
])
async def test_all_demo_users_can_login(
    async_client: AsyncClient, username: str, password: str, expected_role: str
) -> None:
    r = await async_client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200
    assert expected_role in r.json()["user_info"]["roles"]
