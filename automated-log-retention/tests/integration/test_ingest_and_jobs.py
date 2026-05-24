"""Integration tests for ``POST /v1/logs/ingest``, ``GET /v1/files``,
and ``POST /v1/evaluate``.

These tests run the full app under ``ASGITransport`` (same shape as the
lifespan test) so the routes hit the real catalog + filesystem + policy
pipeline. Each test gets its own temp DB + storage root via the fixture
so concurrent runs can't collide.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture
async def app_under_test(tmp_path, monkeypatch) -> AsyncIterator[tuple]:
    """Fresh app instance with per-test DB and storage root.

    Wraps the app in ``LifespanManager`` so the lifespan (DB init,
    scheduler start, policy load) actually fires — httpx's
    ``ASGITransport`` alone doesn't drive lifespan events.
    """
    db_path = tmp_path / "test.db"
    storage_root = tmp_path / "tiers"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv(
        "POLICY_CONFIG_PATH", str(PROJECT_ROOT / "config" / "retention_config.yaml")
    )
    # Long intervals — we drive the lifecycle synchronously via routes.
    monkeypatch.setenv("SCAN_INTERVAL_SEC", "3600")
    monkeypatch.setenv("APPLY_INTERVAL_SEC", "3600")
    monkeypatch.setenv("SWEEP_INTERVAL_SEC", "3600")

    from src.settings import get_settings

    get_settings.cache_clear()
    for mod_name in list(sys.modules.keys()):
        if mod_name == "src.main":
            del sys.modules[mod_name]

    import src.main as main_module  # noqa: E402

    importlib.reload(main_module)
    app = main_module.app

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


def _records(n: int, source: str = "test_source") -> list[dict]:
    """Generate ``n`` valid LogRecord payloads."""
    return [
        {
            "ts": "2026-05-23T00:00:00Z",
            "level": "info",
            "source": source,
            "category": "user_activity",
            "message": f"hello {i}",
        }
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_ingest_returns_accepted_count(app_under_test):
    """A successful ingest echoes the accepted count and a segment path."""
    _app, client = app_under_test
    r = await client.post(
        "/v1/logs/ingest", json={"records": _records(10)}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 10
    assert body["segment_path"].endswith(".jsonl")


@pytest.mark.asyncio
async def test_ingest_and_files_endpoint(app_under_test):
    """After ingest + evaluate, the catalog surfaces the rolled segment."""
    _app, client = app_under_test
    r = await client.post(
        "/v1/logs/ingest", json={"records": _records(10)}
    )
    assert r.status_code == 200

    # Force the open segment to register with the catalog.
    r = await client.post("/v1/evaluate")
    assert r.status_code == 200

    r = await client.get("/v1/files")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 1
    assert len(body["files"]) >= 1
    file = body["files"][0]
    assert file["source"] == "test_source"
    # The file path lives under the test's storage root + the hot tier.
    assert "/hot/" in file["segment_path"]
    assert file["tier"] == "hot"
    assert file["size_bytes"] > 0


@pytest.mark.asyncio
async def test_evaluate_runs_full_cycle(app_under_test):
    """``/v1/evaluate`` returns counts for scan/apply/sweep in one cycle."""
    _app, client = app_under_test
    r = await client.post(
        "/v1/logs/ingest", json={"records": _records(5)}
    )
    assert r.status_code == 200

    r = await client.post("/v1/evaluate")
    assert r.status_code == 200, r.text
    body = r.json()
    # Shape check — we don't assert specific counts because the
    # demo policy's first phase is ``after_days=0 promote target=hot``
    # which is a tier-mismatch no-op when the file is already in hot.
    assert "scanned" in body
    assert "transitions_planned" in body
    assert "applied" in body
    assert "failed" in body
    assert "swept" in body
    assert "eval_seconds" in body
    assert body["eval_seconds"] >= 0


@pytest.mark.asyncio
async def test_evaluate_returns_zero_for_empty_db(app_under_test):
    """A brand-new DB has nothing to scan / apply / sweep."""
    _app, client = app_under_test
    r = await client.post("/v1/evaluate")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scanned"] == 0
    assert body["transitions_planned"] == 0
    assert body["applied"] == 0
    assert body["failed"] == 0
    assert body["swept"] == 0


@pytest.mark.asyncio
async def test_ingest_rejects_empty_records(app_under_test):
    """Empty records list must fail Pydantic validation (422)."""
    _app, client = app_under_test
    r = await client.post("/v1/logs/ingest", json={"records": []})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_ingest_groups_by_source(app_under_test):
    """Two sources in one batch produce two distinct open segments."""
    _app, client = app_under_test
    records = _records(3, source="source_a") + _records(3, source="source_b")
    r = await client.post("/v1/logs/ingest", json={"records": records})
    assert r.status_code == 200

    # Flush so catalog shows both.
    r = await client.post("/v1/evaluate")
    assert r.status_code == 200

    r = await client.get("/v1/files")
    assert r.status_code == 200
    body = r.json()
    sources = {f["source"] for f in body["files"]}
    assert {"source_a", "source_b"}.issubset(sources)


@pytest.mark.asyncio
async def test_files_filter_by_tier(app_under_test):
    """``GET /v1/files?tier=hot`` only returns hot-tier rows."""
    _app, client = app_under_test
    await client.post("/v1/logs/ingest", json={"records": _records(5)})
    await client.post("/v1/evaluate")

    r = await client.get("/v1/files?tier=hot")
    assert r.status_code == 200
    body = r.json()
    for f in body["files"]:
        assert f["tier"] == "hot"

    r = await client.get("/v1/files?tier=cold")
    assert r.status_code == 200
    assert r.json()["files"] == []


@pytest.mark.asyncio
async def test_files_pagination(app_under_test):
    """``limit`` + ``offset`` page consistently through results."""
    _app, client = app_under_test
    # Ingest into 3 distinct sources so 3 rows land.
    for src in ("s1", "s2", "s3"):
        await client.post(
            "/v1/logs/ingest", json={"records": _records(2, source=src)}
        )
    await client.post("/v1/evaluate")

    r = await client.get("/v1/files?limit=2&offset=0")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] >= 3
    assert len(body["files"]) == 2

    r = await client.get("/v1/files?limit=2&offset=2")
    assert r.status_code == 200
    body = r.json()
    assert len(body["files"]) >= 1
