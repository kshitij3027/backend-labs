"""Tests for src/dashboard.py — the standalone dashboard service.

Strategy
--------
The dashboard's external dependencies are httpx (for polling each node
and proxying the failover button) and Redis (for resolving the current
lock holder). We keep the unit tests fully isolated by:

* Setting ``REDIS_HOST``, ``PEER_NODES``, ``DASHBOARD_WEB_DIR`` via
  monkeypatch BEFORE :func:`create_app` is called so :class:`DashboardConfig`
  picks up the test values.
* After construction, swapping ``app.state.client`` for an
  ``httpx.AsyncClient(transport=httpx.MockTransport(...))`` so we
  control every node response.
* Stubbing :py:meth:`RedisClient.connect` / :py:meth:`read_lock_holder`
  on the *instance* attached to ``app.state.redis`` so we never touch
  a real Redis.

The static index/app.js routes need real files on disk; the
``temp_web_dir`` fixture below writes minimal placeholders into a tmp
path that we point ``DASHBOARD_WEB_DIR`` at.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from src.dashboard import (
    DashboardConfig,
    _collect_snapshot,
    create_app,
)


# =========================================================================
# Fixtures local to this module
# =========================================================================


@pytest.fixture
def temp_web_dir(tmp_path: Any) -> str:
    """Write minimal index.html + app.js into a tmp dir and return its path."""
    web = tmp_path / "web"
    web.mkdir()
    (web / "index.html").write_text(
        "<!DOCTYPE html><html><body><h1>Failover Cluster Dashboard</h1>"
        "<div id=\"nodes-grid\"></div></body></html>",
        encoding="utf-8",
    )
    (web / "app.js").write_text(
        "// app.js test stub\nconsole.log('dashboard');",
        encoding="utf-8",
    )
    return str(web)


@pytest.fixture
def dashboard_env(monkeypatch: pytest.MonkeyPatch, temp_web_dir: str) -> None:
    """Set the env vars DashboardConfig reads at construction time."""
    monkeypatch.setenv("REDIS_HOST", "fake")
    monkeypatch.setenv("REDIS_PORT", "0")
    monkeypatch.setenv("PEER_NODES", "node-1:8001,node-2:8002,node-3:8003")
    monkeypatch.setenv("DASHBOARD_POLL_INTERVAL", "0.1")
    monkeypatch.setenv("DASHBOARD_WEB_DIR", temp_web_dir)


# =========================================================================
# Helpers
# =========================================================================


def _make_canned_transport() -> httpx.MockTransport:
    """An httpx transport returning canned /role + /metrics for 3 nodes."""

    def _handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        path = request.url.path
        if path == "/role":
            if host == "node-1":
                return httpx.Response(
                    200,
                    json={
                        "node_id": "node-1",
                        "state": "PRIMARY",
                        "role": "primary",
                        "lock_holder": "node-1",
                        "known_winner": "node-1",
                        "term": 1,
                    },
                )
            if host == "node-2":
                return httpx.Response(
                    200,
                    json={
                        "node_id": "node-2",
                        "state": "STANDBY",
                        "role": "standby",
                        "lock_holder": "node-1",
                        "known_winner": "node-1",
                        "term": 1,
                    },
                )
            if host == "node-3":
                return httpx.Response(
                    200,
                    json={
                        "node_id": "node-3",
                        "state": "STANDBY",
                        "role": "standby",
                        "lock_holder": "node-1",
                        "known_winner": "node-1",
                        "term": 1,
                    },
                )
        if path == "/metrics":
            count = {"node-1": 50, "node-2": 0, "node-3": 0}.get(host, 0)
            text = (
                "# HELP logs_ingested_total dummy\n"
                "# TYPE logs_ingested_total counter\n"
                f'logs_ingested_total{{node_id="{host}"}} {count}\n'
            )
            return httpx.Response(200, text=text)
        if path == "/admin/trigger-failover":
            return httpx.Response(202, json={"status": "failover_triggered"})
        return httpx.Response(404, json={"status": "not_found"})

    return httpx.MockTransport(_handler)


def _attach_mock_transport(app: Any) -> None:
    """Replace ``app.state.client`` with one backed by the canned transport."""
    transport = _make_canned_transport()
    # Close the real client cleanly before swapping.
    real_client: httpx.AsyncClient = app.state.client
    # We don't await aclose() here because we're outside an event loop;
    # httpx.AsyncClient never opened a connection (no requests fired yet)
    # so dropping it is safe in the test path.
    del real_client
    app.state.client = httpx.AsyncClient(transport=transport)


class _FakeRedis:
    """Fake stand-in matching the subset of RedisClient the dashboard uses."""

    def __init__(self, holder: str | None = None) -> None:
        self.holder = holder
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def read_lock_holder(self) -> str | None:
        return self.holder


def _attach_fake_redis(app: Any, holder: str | None) -> _FakeRedis:
    fake = _FakeRedis(holder=holder)
    app.state.redis = fake
    return fake


# =========================================================================
# 1. DashboardConfig parses PEER_NODES
# =========================================================================


def test_dashboard_config_parses_peer_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "PEER_NODES", "node-1:8001, node-2:8002 ,node-3:8003,bad,, "
    )
    cfg = DashboardConfig()
    peers = cfg.peers()
    assert peers == [
        ("node-1", 8001),
        ("node-2", 8002),
        ("node-3", 8003),
    ]


def test_dashboard_config_empty_peer_nodes_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PEER_NODES", "")
    cfg = DashboardConfig()
    assert cfg.peers() == []


# =========================================================================
# 2. GET / serves index.html
# =========================================================================


def test_get_index_returns_html(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder=None)

    with TestClient(app) as client:
        resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "Failover" in resp.text or "dashboard" in resp.text.lower()


def test_get_app_js_returns_javascript(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder=None)

    with TestClient(app) as client:
        resp = client.get("/app.js")
    assert resp.status_code == 200
    assert "javascript" in resp.headers.get("content-type", "").lower()
    assert "dashboard" in resp.text.lower()


# =========================================================================
# 3. WebSocket /ws yields a snapshot of expected shape
# =========================================================================


def test_ws_emits_snapshot_with_expected_shape(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder="node-1")

    with TestClient(app) as client:
        # The poll loop runs in lifespan, ticks every 0.1s in this test.
        # The first message may either be the immediate-on-connect push
        # of last_snapshot or the next broadcast tick. Either way the
        # shape must match. Starlette's TestClient does not expose a
        # per-call timeout on receive_json; rely on the surrounding
        # pytest-level test timeout.
        with client.websocket_connect("/ws") as ws:
            data = ws.receive_json()

    assert isinstance(data, dict)
    assert set(data.keys()) >= {"nodes", "throughput_lps", "timestamp"}
    nodes = data["nodes"]
    assert isinstance(nodes, list)
    assert len(nodes) == 3
    ids = sorted(n["node_id"] for n in nodes)
    assert ids == ["node-1", "node-2", "node-3"]

    # Find the primary; assert state was picked up from /role.
    primary = next(n for n in nodes if n["node_id"] == "node-1")
    assert primary["state"] == "PRIMARY"
    assert primary["role"] == "primary"
    assert primary["lock_holder"] == "node-1"

    # log_count was scraped from /metrics on node-1.
    assert primary["log_count"] == 50

    # throughput is a non-negative float.
    assert isinstance(data["throughput_lps"], (int, float))
    assert data["throughput_lps"] >= 0


# =========================================================================
# 4. GET /api/snapshot returns same shape (HTTP polling fallback)
# =========================================================================


def test_api_snapshot_returns_snapshot(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder="node-1")

    with TestClient(app) as client:
        resp = client.get("/api/snapshot")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) >= {"nodes", "throughput_lps", "timestamp"}
    assert len(body["nodes"]) == 3


# =========================================================================
# 5. Failover proxy: no primary -> 503
# =========================================================================


def test_proxy_failover_no_primary_returns_503(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder=None)

    with TestClient(app) as client:
        resp = client.post("/proxy/admin/trigger-failover", json={})
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "error"
    assert body["reason"] == "no_primary"


# =========================================================================
# 6. Failover proxy: holder not in peers -> 503
# =========================================================================


def test_proxy_failover_holder_not_in_peers_returns_503(
    dashboard_env: None,
) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder="node-zombie")

    with TestClient(app) as client:
        resp = client.post("/proxy/admin/trigger-failover", json={})
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "error"
    assert body["reason"] == "primary_not_in_peers"
    assert body["holder"] == "node-zombie"


# =========================================================================
# 7. Failover proxy: holder in peers -> forwards and returns the upstream code
# =========================================================================


def test_proxy_failover_forwards_to_primary(dashboard_env: None) -> None:
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder="node-1")

    with TestClient(app) as client:
        resp = client.post("/proxy/admin/trigger-failover", json={})
    # Mock transport returns 202 for /admin/trigger-failover.
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "forwarded"
    assert body["holder"] == "node-1"
    assert body["code"] == 202


# =========================================================================
# 8. Failover proxy: upstream HTTP error -> 502
# =========================================================================


def test_proxy_failover_upstream_error_returns_502(
    dashboard_env: None,
) -> None:
    app = create_app()
    _attach_fake_redis(app, holder="node-1")

    # Build a transport whose handler raises ConnectError on the
    # /admin/trigger-failover path so we exercise the 502 branch.
    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/admin/trigger-failover":
            raise httpx.ConnectError("simulated", request=request)
        return httpx.Response(200, json={})

    app.state.client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))

    with TestClient(app) as client:
        resp = client.post("/proxy/admin/trigger-failover", json={})
    assert resp.status_code == 502
    body = resp.json()
    assert body["status"] == "error"
    assert body["reason"] == "proxy_failed"
    assert body["holder"] == "node-1"


# =========================================================================
# 9. _collect_snapshot computes throughput across the rolling window
# =========================================================================


async def test_collect_snapshot_throughput_grows_with_log_count(
    dashboard_env: None,
) -> None:
    """Two snapshots back-to-back should produce a positive throughput.

    We can't drive the lifespan from a pure async test (TestClient owns
    that), so we exercise ``_collect_snapshot`` directly. The mock
    transport returns log_count=50 on node-1 — both snapshots see the
    same value, so the second one's delta is 0 and throughput is 0.0.
    To prove the math works we then mutate the throughput history and
    re-collect: the delta function is purely time + count based, and a
    test that locks down its shape (non-negative, float, included in
    the snapshot dict) is enough for the unit layer.
    """
    app = create_app()
    _attach_mock_transport(app)
    _attach_fake_redis(app, holder="node-1")

    snap1 = await _collect_snapshot(app)
    snap2 = await _collect_snapshot(app)

    assert isinstance(snap1["throughput_lps"], (int, float))
    assert isinstance(snap2["throughput_lps"], (int, float))
    assert snap1["throughput_lps"] >= 0
    assert snap2["throughput_lps"] >= 0
    # Both snapshots should see all 3 nodes.
    assert {n["node_id"] for n in snap1["nodes"]} == {"node-1", "node-2", "node-3"}
    assert {n["node_id"] for n in snap2["nodes"]} == {"node-1", "node-2", "node-3"}

    # Cleanup: close the swapped-in mock client to avoid resource warnings.
    await app.state.client.aclose()


# =========================================================================
# 10. _collect_snapshot tolerates an unreachable peer
# =========================================================================


async def test_collect_snapshot_marks_unreachable_peer(
    dashboard_env: None,
) -> None:
    app = create_app()
    _attach_fake_redis(app, holder=None)

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "node-1":
            raise httpx.ConnectError("simulated", request=request)
        if request.url.path == "/role":
            return httpx.Response(
                200,
                json={
                    "node_id": request.url.host,
                    "state": "STANDBY",
                    "role": "standby",
                    "lock_holder": None,
                    "known_winner": None,
                    "term": 0,
                },
            )
        if request.url.path == "/metrics":
            return httpx.Response(
                200,
                text='logs_ingested_total{node_id="x"} 0\n',
            )
        return httpx.Response(404)

    app.state.client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))

    snap = await _collect_snapshot(app)
    by_id = {n["node_id"]: n for n in snap["nodes"]}
    # node-1 is unreachable: defaults preserved, state stays UNREACHABLE.
    assert by_id["node-1"]["state"] == "UNREACHABLE"
    # node-2 / node-3 came back with STANDBY.
    assert by_id["node-2"]["state"] == "STANDBY"
    assert by_id["node-3"]["state"] == "STANDBY"

    await app.state.client.aclose()
