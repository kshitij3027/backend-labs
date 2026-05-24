"""Integration tests covering the C12 lifespan wiring.

These tests exercise the full FastAPI startup: DB init, policy load,
scheduler registration, ingest-buffer + lock attachment. They use an
``httpx.AsyncClient(transport=ASGITransport(...))`` so the app's
``lifespan`` context fires exactly as it would under uvicorn — no real
port binding, no subprocesses.

The fixture monkeypatches ``DATABASE_URL`` and ``STORAGE_ROOT`` to
per-test temporary paths so a hot rerun doesn't trip over WAL files
from a previous run, and clears the ``get_settings`` ``lru_cache`` so
the new env wins. The app module is re-imported per test for the same
reason (FastAPI's lifespan binds the settings once at app-construct
time).
"""
from __future__ import annotations

import asyncio
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
    """Spin up a fresh ``src.main:app`` against a per-test SQLite DB.

    Yields ``(app, client)``. The ``LifespanManager`` is what actually
    fires the FastAPI ``lifespan`` (httpx 0.27's ``ASGITransport`` does
    not on its own — see ``asgi-lifespan`` README). Both manager + client
    tear down on exit, which is how the scheduler gets a clean shutdown.
    """
    db_path = tmp_path / "test.db"
    storage_root = tmp_path / "tiers"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv(
        "POLICY_CONFIG_PATH", str(PROJECT_ROOT / "config" / "retention_config.yaml")
    )
    # Long intervals so the background scheduler doesn't fire during the
    # test window — we exercise the lifecycle synchronously via routes
    # / direct calls, not via tick timing.
    monkeypatch.setenv("SCAN_INTERVAL_SEC", "3600")
    monkeypatch.setenv("APPLY_INTERVAL_SEC", "3600")
    monkeypatch.setenv("SWEEP_INTERVAL_SEC", "3600")

    # Drop cached settings + the previously-imported app so the new env
    # is observed and a fresh app object is constructed.
    from src.settings import get_settings

    get_settings.cache_clear()
    for mod_name in list(sys.modules.keys()):
        if mod_name == "src.main" or mod_name.startswith("src.main."):
            del sys.modules[mod_name]

    import src.main as main_module  # noqa: E402

    importlib.reload(main_module)
    app = main_module.app

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


@pytest.mark.asyncio
async def test_lifespan_starts_scheduler_with_three_jobs(app_under_test):
    """Scheduler must be wired with at least the 3 lifecycle jobs."""
    app, _client = app_under_test
    jobs = app.state.scheduler.get_jobs()
    job_ids = {j.id for j in jobs}
    assert {"scan_job", "apply_job", "sweep_job"}.issubset(
        job_ids
    ), f"expected scan/apply/sweep in scheduler; got {job_ids}"
    assert app.state.scheduler.running is True


@pytest.mark.asyncio
async def test_health_still_works(app_under_test):
    """The C01 health route survives the lifespan rewrite."""
    _app, client = app_under_test
    r = await client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert isinstance(body["timestamp"], int)
    assert body["timestamp"] > 0


@pytest.mark.asyncio
async def test_policy_set_loaded(app_under_test):
    """The policy YAML loads and lands on ``app.state.policy_set``."""
    app, _client = app_under_test
    assert app.state.policy_set is not None
    assert len(app.state.policy_set.policies) > 0


@pytest.mark.asyncio
async def test_ingest_buffer_and_lock_attached(app_under_test):
    """Lifespan attaches a fresh ingest buffer + asyncio.Lock."""
    app, _client = app_under_test
    assert isinstance(app.state.ingest_buffer, dict)
    assert app.state.ingest_buffer == {}
    assert isinstance(app.state.ingest_lock, asyncio.Lock)


@pytest.mark.asyncio
async def test_tier_directories_created(app_under_test, tmp_path):
    """Lifespan creates all 5 tier dirs under STORAGE_ROOT."""
    app, _client = app_under_test
    storage_root = Path(app.state.settings.storage_root)
    for tier in ("hot", "warm", "cold", "archive", "pending"):
        assert (storage_root / tier).is_dir(), f"missing tier dir: {tier}"
