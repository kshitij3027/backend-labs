from __future__ import annotations

import os
from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def app_instance() -> FastAPI:
    os.environ.setdefault("SECRET_KEY", "test-secret-key-please-change")
    from src.main import build_app

    return build_app()


@pytest_asyncio.fixture
async def async_client(app_instance: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(scope="session")
def live_url() -> str:
    return os.getenv("API_URL", "http://api:8000")
