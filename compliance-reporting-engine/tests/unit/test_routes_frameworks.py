"""Unit tests for :mod:`src.api.routes_frameworks`.

The frameworks endpoint is a read-only catalogue served from the
in-process registry. Tests mount only that router on a fresh FastAPI
app — no DB, no coordinator wiring — and use ``httpx.AsyncClient +
ASGITransport`` so we can exercise it without a network port.

After commit 7 the registry holds exactly the four "core" frameworks
(SOX, HIPAA, PCI_DSS, GDPR). FinHealth lands in commit 17, so this
test asserts ``>= 4`` rather than exactly 4 to avoid breaking when
the FinHealth module is added later.
"""
from __future__ import annotations

from typing import AsyncIterator

import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.api import routes_frameworks
from src.frameworks import FRAMEWORK_REGISTRY


# Frameworks present at commit 13 (FinHealth arrives later in commit 17).
_CORE_FRAMEWORKS = {"SOX", "HIPAA", "PCI_DSS", "GDPR"}


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Minimal app + AsyncClient — the frameworks router has no DB deps."""
    app = FastAPI()
    app.include_router(routes_frameworks.router)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def test_get_frameworks_returns_at_least_four(client) -> None:
    """SOX / HIPAA / PCI_DSS / GDPR are all present in the catalogue.

    Asserting ``>= 4`` (rather than exactly 4) lets FinHealth land in
    commit 17 without retroactively breaking this test. The plan says
    "4 entries (5 once FinHealth lands)" — the inclusive lower bound
    captures both states without a conditional.
    """
    response = await client.get("/frameworks")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) >= 4
    names = {entry["name"] for entry in body}
    assert _CORE_FRAMEWORKS.issubset(names), names
    # The endpoint must mirror the live registry — no hardcoded list.
    assert names == set(FRAMEWORK_REGISTRY.keys())


async def test_each_framework_has_categories_and_description(client) -> None:
    """Every entry carries a non-empty categories list AND a description."""
    response = await client.get("/frameworks")
    assert response.status_code == 200
    body = response.json()
    for entry in body:
        # Name is a non-empty string.
        assert isinstance(entry["name"], str) and entry["name"]
        # Categories: non-empty list of strings (the 5-category contract).
        assert isinstance(entry["categories"], list)
        assert len(entry["categories"]) >= 1
        for cat in entry["categories"]:
            assert isinstance(cat, str) and cat
        # Description: present and non-empty.
        assert isinstance(entry["description"], str) and entry["description"]
