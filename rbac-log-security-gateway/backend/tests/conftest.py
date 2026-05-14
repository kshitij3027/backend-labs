"""Shared pytest fixtures. JWT_SECRET_KEY is injected at module load before any src.* import."""
from __future__ import annotations

import os

# CRITICAL: this MUST run before any src.* import so Settings can construct.
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-please-change")
os.environ.setdefault("JWT_ALGORITHM", "HS256")
os.environ.setdefault("JWT_EXPIRY_MINUTES", "60")
os.environ.setdefault("CORS_ALLOWED_ORIGINS", "http://localhost:3000")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from httpx import AsyncClient, ASGITransport  # noqa: E402


@pytest.fixture(scope="session")
def app_instance() -> FastAPI:
    """Return the FastAPI app — imports are deferred so env vars take effect."""
    from src.main import build_app  # noqa: WPS433

    return build_app()


@pytest.fixture
async def async_client(app_instance: FastAPI) -> AsyncClient:
    """An httpx AsyncClient wired to the ASGI app, no network."""
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(scope="session")
def admin_token(app_instance: FastAPI) -> str:
    from src.auth.service import AuthService
    token, _exp, _user = AuthService().login("alice", "admin123")
    return token


@pytest.fixture(scope="session")
def dev_token(app_instance: FastAPI) -> str:
    from src.auth.service import AuthService
    token, _exp, _user = AuthService().login("bob", "dev123")
    return token


@pytest.fixture(scope="session")
def analyst_token(app_instance: FastAPI) -> str:
    from src.auth.service import AuthService
    token, _exp, _user = AuthService().login("carol", "analyst123")
    return token


@pytest.fixture(scope="session")
def support_token(app_instance: FastAPI) -> str:
    from src.auth.service import AuthService
    token, _exp, _user = AuthService().login("dave", "support123")
    return token
