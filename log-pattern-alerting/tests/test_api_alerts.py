"""Tests for the Alert Lifecycle REST API endpoints."""

from datetime import datetime, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.database import get_db
from src.main import app
from src.models import Alert, AlertRule, AlertState, Base
from src.websocket import ConnectionManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_alert(
    pattern_name: str = "test_pattern",
    severity: str = "high",
    message: str = "Test alert message",
    state: str = AlertState.NEW.value,
    count: int = 1,
    acknowledged_by: str | None = None,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
) -> Alert:
    """Create an Alert model instance with sensible defaults."""
    now = datetime.utcnow()
    return Alert(
        pattern_name=pattern_name,
        severity=severity,
        message=message,
        state=state,
        count=count,
        first_occurrence=now,
        last_occurrence=now,
        acknowledged_by=acknowledged_by,
        acknowledged_at=acknowledged_at,
        resolved_at=resolved_at,
    )


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
async def client(async_engine, test_session):
    """Provide an httpx AsyncClient with the app's get_db overridden
    to use the test database session, and clean tables between tests."""

    # Clean before test
    async with async_engine.begin() as conn:
        await conn.execute(text("DELETE FROM alerts"))
        await conn.execute(text("DELETE FROM alert_rules"))

    # Override FastAPI's get_db dependency to use our test session
    async def _override_get_db():
        yield test_session

    app.dependency_overrides[get_db] = _override_get_db

    # Wire up a ConnectionManager on app.state so the acknowledge/resolve
    # endpoints can broadcast via WebSocket without a running lifespan.
    ws_manager = ConnectionManager()
    app.state.connection_manager = ws_manager

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    # Restore and clean after test
    app.dependency_overrides.clear()
    if hasattr(app.state, "connection_manager"):
        del app.state.connection_manager
    async with async_engine.begin() as conn:
        await conn.execute(text("DELETE FROM alerts"))
        await conn.execute(text("DELETE FROM alert_rules"))


# ---------------------------------------------------------------------------
# GET /alerts
# ---------------------------------------------------------------------------

class TestListAlerts:
    async def test_empty_list(self, client: AsyncClient):
        resp = await client.get("/alerts")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_returns_inserted_alert(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(pattern_name="auth_failure", severity="critical")
        test_session.add(alert)
        await test_session.commit()

        resp = await client.get("/alerts")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["pattern_name"] == "auth_failure"
        assert data[0]["severity"] == "critical"
        assert data[0]["state"] == "NEW"

    async def test_filter_by_state(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        test_session.add(_make_alert(pattern_name="a", state=AlertState.NEW.value))
        test_session.add(_make_alert(pattern_name="b", state=AlertState.ACKNOWLEDGED.value))
        test_session.add(_make_alert(pattern_name="c", state=AlertState.RESOLVED.value))
        await test_session.commit()

        resp = await client.get("/alerts", params={"state": "NEW"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["pattern_name"] == "a"

    async def test_filter_invalid_state(self, client: AsyncClient):
        resp = await client.get("/alerts", params={"state": "BOGUS"})
        assert resp.status_code == 400
        assert "Invalid state" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /alerts/{id}
# ---------------------------------------------------------------------------

class TestGetAlert:
    async def test_found(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(pattern_name="disk_full")
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.get(f"/alerts/{alert.id}")
        assert resp.status_code == 200
        assert resp.json()["pattern_name"] == "disk_full"
        assert resp.json()["id"] == alert.id

    async def test_not_found(self, client: AsyncClient):
        resp = await client.get("/alerts/99999")
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Alert not found"


# ---------------------------------------------------------------------------
# POST /alerts/{id}/acknowledge
# ---------------------------------------------------------------------------

class TestAcknowledgeAlert:
    async def test_acknowledge_new_alert(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(state=AlertState.NEW.value)
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.post(
            f"/alerts/{alert.id}/acknowledge",
            json={"acknowledged_by": "admin"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "ACKNOWLEDGED"
        assert body["acknowledged_by"] == "admin"
        assert body["acknowledged_at"] is not None

    async def test_acknowledge_resolved_alert_returns_400(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(
            state=AlertState.RESOLVED.value,
            resolved_at=datetime.utcnow(),
        )
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.post(
            f"/alerts/{alert.id}/acknowledge",
            json={"acknowledged_by": "admin"},
        )
        assert resp.status_code == 400
        assert "resolved" in resp.json()["detail"].lower()

    async def test_acknowledge_nonexistent_returns_404(self, client: AsyncClient):
        resp = await client.post(
            "/alerts/99999/acknowledge",
            json={"acknowledged_by": "admin"},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /alerts/{id}/resolve
# ---------------------------------------------------------------------------

class TestResolveAlert:
    async def test_resolve_new_alert(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(state=AlertState.NEW.value)
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.post(f"/alerts/{alert.id}/resolve")
        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "RESOLVED"
        assert body["resolved_at"] is not None

    async def test_resolve_acknowledged_alert(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(state=AlertState.ACKNOWLEDGED.value)
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.post(f"/alerts/{alert.id}/resolve")
        assert resp.status_code == 200
        assert resp.json()["state"] == "RESOLVED"

    async def test_resolve_already_resolved_returns_400(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        alert = _make_alert(
            state=AlertState.RESOLVED.value,
            resolved_at=datetime.utcnow(),
        )
        test_session.add(alert)
        await test_session.commit()
        await test_session.refresh(alert)

        resp = await client.post(f"/alerts/{alert.id}/resolve")
        assert resp.status_code == 400
        assert "already resolved" in resp.json()["detail"].lower()

    async def test_resolve_nonexistent_returns_404(self, client: AsyncClient):
        resp = await client.post("/alerts/99999/resolve")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /stats
# ---------------------------------------------------------------------------

class TestStats:
    async def test_empty_stats(self, client: AsyncClient):
        resp = await client.get("/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active_alerts"] == 0
        assert body["total_patterns"] == 0
        assert body["alerts_by_severity"] == {}

    async def test_stats_counts(
        self, client: AsyncClient, test_session: AsyncSession,
    ):
        # 2 active alerts (NEW, ACKNOWLEDGED), 1 resolved
        test_session.add(_make_alert(pattern_name="a", severity="high", state=AlertState.NEW.value))
        test_session.add(_make_alert(pattern_name="b", severity="high", state=AlertState.ACKNOWLEDGED.value))
        test_session.add(_make_alert(pattern_name="c", severity="low", state=AlertState.RESOLVED.value))

        # 2 enabled rules, 1 disabled
        test_session.add(AlertRule(
            name="rule1", pattern="error.*", threshold=5,
            window_seconds=60, severity="high", enabled=True,
        ))
        test_session.add(AlertRule(
            name="rule2", pattern="warn.*", threshold=10,
            window_seconds=120, severity="low", enabled=True,
        ))
        test_session.add(AlertRule(
            name="rule3", pattern="info.*", threshold=100,
            window_seconds=300, severity="low", enabled=False,
        ))
        await test_session.commit()

        resp = await client.get("/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert body["active_alerts"] == 2
        assert body["total_patterns"] == 2
        assert body["alerts_by_severity"]["high"] == 2
