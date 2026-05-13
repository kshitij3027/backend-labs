"""Unit tests for C15 --- ``/admin/circuit-breaker*`` endpoints.

These endpoints read the supervisor off ``app.state``; if it's missing
the routes 503 (the lifespan installs it, so production never hits 503
--- it's a unit-test honesty guard).

We build a minimal FastAPI app, wire ``routes_admin.router`` only, and
populate ``app.state.supervisor`` with a ``MagicMock`` so we don't drag
in any of the lifespan dependencies (Docker, monitor, DB).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes_admin import router as admin_router


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _make_app_with_supervisor(supervisor: object | None) -> FastAPI:
    """Build a FastAPI app wired to ``routes_admin`` only.

    ``app.state.supervisor`` is set to ``supervisor``; if ``None``,
    the attribute is intentionally not set so the routes hit the
    ``getattr(...) is None`` 503 path.
    """
    app = FastAPI()
    app.include_router(admin_router)
    if supervisor is not None:
        app.state.supervisor = supervisor
    return app


def _make_supervisor_mock(state_payload: dict) -> MagicMock:
    """Mock that quacks like a SafetySupervisor for the admin routes.

    ``state.to_dict`` and ``reset`` both return ``state_payload`` so the
    tests can assert the body equals the dict the supervisor handed back.
    """
    sup = MagicMock()
    sup.state.to_dict = MagicMock(return_value=state_payload)
    # ``reset()`` returns the CircuitBreakerState object; the route then
    # calls ``.to_dict()`` on the result. We model that here.
    reset_result = MagicMock()
    reset_result.to_dict = MagicMock(return_value=state_payload)
    sup.reset = MagicMock(return_value=reset_result)
    return sup


# --------------------------------------------------------------------------- #
# GET /admin/circuit-breaker-state
# --------------------------------------------------------------------------- #


class TestCircuitBreakerStateGet:
    def test_returns_supervisor_state_dict(self) -> None:
        payload = {
            "tripped": False,
            "reason": None,
            "tripped_at": None,
            "last_breach_metric": None,
            "consecutive_breach_count": 0,
            "total_trips": 0,
        }
        supervisor = _make_supervisor_mock(payload)
        app = _make_app_with_supervisor(supervisor)
        with TestClient(app) as client:
            resp = client.get("/admin/circuit-breaker-state")
        assert resp.status_code == 200
        assert resp.json() == payload
        supervisor.state.to_dict.assert_called_once()

    def test_returns_503_when_supervisor_missing(self) -> None:
        app = _make_app_with_supervisor(None)
        with TestClient(app) as client:
            resp = client.get("/admin/circuit-breaker-state")
        assert resp.status_code == 503
        assert "supervisor not wired" in resp.json()["detail"]


# --------------------------------------------------------------------------- #
# POST /admin/circuit-breaker/reset
# --------------------------------------------------------------------------- #


class TestCircuitBreakerResetPost:
    def test_calls_reset_and_returns_its_dict(self) -> None:
        payload = {
            "tripped": False,
            "reason": None,
            "tripped_at": None,
            "last_breach_metric": None,
            "consecutive_breach_count": 0,
            "total_trips": 3,
        }
        supervisor = _make_supervisor_mock(payload)
        app = _make_app_with_supervisor(supervisor)
        with TestClient(app) as client:
            resp = client.post("/admin/circuit-breaker/reset")
        assert resp.status_code == 200
        assert resp.json() == payload
        supervisor.reset.assert_called_once()

    def test_returns_503_when_supervisor_missing(self) -> None:
        app = _make_app_with_supervisor(None)
        with TestClient(app) as client:
            resp = client.post("/admin/circuit-breaker/reset")
        assert resp.status_code == 503
        assert "supervisor not wired" in resp.json()["detail"]
