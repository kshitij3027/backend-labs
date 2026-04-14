"""Tests for the Log Injection REST API (POST /test/inject_log)."""

import pytest
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database import get_db
from src.engine.correlation import AlertCorrelator
from src.engine.pattern_matcher import PatternMatcher
from src.engine.pipeline import AlertPipeline
from src.engine.rate_limiter import RateLimiter
from src.main import app
from src.models import AlertRule, LogEntry
from src.websocket import ConnectionManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def test_session(async_engine):
    """Create an async session bound to the test engine."""
    factory = async_sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False,
    )
    async with factory() as session:
        yield session


@pytest.fixture
async def client(async_engine, test_session, redis_client):
    """Provide an httpx AsyncClient wired to the test database.

    Seeds a minimal AlertRule, wires up the pipeline on app.state
    so the inject_log endpoint works, then cleans everything after.
    """
    session_factory = async_sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False,
    )

    # Clean before test
    async with async_engine.begin() as conn:
        await conn.execute(text("DELETE FROM alerts"))
        await conn.execute(text("DELETE FROM alert_rules"))
        await conn.execute(text("DELETE FROM log_entries"))

    # Seed a rule via ORM so created_at default is populated
    async with session_factory() as seed_session:
        seed_session.add(AlertRule(
            name="auth_failure",
            pattern=r"authentication\s+failed|login\s+failed|auth\s+error",
            threshold=5,
            window_seconds=60,
            severity="high",
            enabled=True,
        ))
        await seed_session.commit()

    # Build a real pipeline and attach to app.state
    matcher = PatternMatcher()
    correlator = AlertCorrelator(correlation_window=300)
    rate_limiter = RateLimiter(redis_client, max_per_minute=10)
    ws_manager = ConnectionManager()

    pipeline = AlertPipeline(
        pattern_matcher=matcher,
        correlator=correlator,
        rate_limiter=rate_limiter,
        connection_manager=ws_manager,
        session_factory=session_factory,
    )

    async with session_factory() as init_session:
        await pipeline.initialize(init_session)

    app.state.pipeline = pipeline
    app.state.connection_manager = ws_manager

    # Override the get_db dependency
    async def _override_get_db():
        yield test_session

    app.dependency_overrides[get_db] = _override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    # Restore and clean
    app.dependency_overrides.clear()
    # Clean up app.state attributes we set
    if hasattr(app.state, "pipeline"):
        del app.state.pipeline
    if hasattr(app.state, "connection_manager"):
        del app.state.connection_manager

    async with async_engine.begin() as conn:
        await conn.execute(text("DELETE FROM alerts"))
        await conn.execute(text("DELETE FROM alert_rules"))
        await conn.execute(text("DELETE FROM log_entries"))


# ---------------------------------------------------------------------------
# POST /test/inject_log
# ---------------------------------------------------------------------------

class TestInjectLog:
    async def test_inject_returns_200(self, client: AsyncClient):
        resp = await client.post(
            "/test/inject_log",
            json={"message": "auth error detected", "level": "ERROR"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "processed"
        assert "log_id" in body
        assert body["patterns_matched"] >= 0

    async def test_missing_message_returns_422(self, client: AsyncClient):
        resp = await client.post(
            "/test/inject_log",
            json={"level": "ERROR"},
        )
        assert resp.status_code == 422

    async def test_log_stored_in_db(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        resp = await client.post(
            "/test/inject_log",
            json={
                "message": "auth error something happened",
                "level": "WARNING",
                "source": "test-suite",
            },
        )
        assert resp.status_code == 200
        log_id = resp.json()["log_id"]

        result = await test_session.execute(
            select(LogEntry).where(LogEntry.id == log_id)
        )
        entry = result.scalar_one_or_none()
        assert entry is not None
        assert entry.message == "auth error something happened"
        assert entry.level == "WARNING"
        assert entry.source == "test-suite"
        assert entry.processed is True
