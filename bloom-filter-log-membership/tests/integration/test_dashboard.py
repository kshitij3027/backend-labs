"""Integration tests for the real-time dashboard process (C12).

These run the SEPARATE dashboard app (``src.dashboard:app``) under
Starlette's ``TestClient`` — never the membership API. The dashboard's only
window into the service is HTTP, and both of its outbound seams are
module-level symbols designed to be monkeypatched here:

* ``src.dashboard.fetch_all_stats`` — the poll loop's (and the per-connect
  immediate tick's) stats source. **Must be patched BEFORE ``TestClient``
  enters**: the lifespan starts the poll-and-broadcast task immediately and
  its first fetch fires before the ``with`` body runs, so a late patch
  would let one real HTTP attempt (to a nonexistent API) through.
* ``src.dashboard.proxy_post`` — the single forwarding primitive behind all
  three ``/proxy/*`` routes, patched to record forwarded URLs/payloads and
  to simulate connection failures.

``DASHBOARD_REFRESH_MS`` is parked at 100 ms (via env + settings-cache
clear, mirroring the repo's fixture convention) so the periodic-tick test
observes multiple loop broadcasts inside a fraction of a second.
"""
from __future__ import annotations

import time
from collections.abc import Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

import src.dashboard as dashboard
from src.settings import get_settings

#: Test-time tick cadence (ms) — fast enough that "wait one tick" costs ~0.1s.
REFRESH_MS = 100

#: Every filter the manager hosts (C11 added ``sessions``); the tick's
#: ``api.filters`` block and the page's stat cards must carry all four.
FILTER_NAMES = ["error_logs", "access_logs", "security_logs", "sessions"]


# --------------------------------------------------------------------------- #
# canned payloads — mimic the real /stats, /pipeline/stats, /sessions/stats   #
# shapes (field names match src.api so the JS contract is exercised honestly) #
# --------------------------------------------------------------------------- #


def _filter_block(elements: int = 1200) -> dict:
    """One per-filter block in the shape ``GET /stats`` reports (C8)."""
    memory_bytes = 1_198_136
    return {
        "elements_added": elements,
        "capacity": 1_000_000,
        "slice_count": 1,
        "rotations": 0,
        "previous_count": 0,
        "memory_bytes": memory_bytes,
        "memory_mb": round(memory_bytes / (1024 * 1024), 3),
        "fill_ratio": 0.0008,
        "estimated_fp_rate": 0.000013,
        "target_fp_rate": 0.01,
        "adds_total": elements,
        "queries_total": 64,
        "positives": 40,
        "negatives": 24,
        "observed_false_positives": 0,
        "observed_fp_rate": 0.0,
        "avg_add_ms": 0.004,
        "p99_add_ms": 0.012,
        "avg_query_ms": 0.003,
        "p50_query_ms": 0.002,
        "p99_query_ms": 0.009,
        "created_at": 1_749_400_000.0,
        "generation_age_seconds": 42.5,
    }


def _pipeline_block() -> dict:
    """One per-filter block in the shape ``GET /pipeline/stats`` reports (C10)."""
    return {
        "storage_rows": 30,
        "lookups": 20,
        "bloom_negatives": 10,
        "storage_skipped_pct": 50.0,
        "storage_hits": 8,
        "false_positives": 2,
        "observed_fp_rate": 0.2,
        "fallback_active": False,
        "fallback_lookups": 0,
        "rotations_triggered": 0,
    }


def canned_payload() -> dict:
    """A full successful fetch result, shaped like ``fetch_all_stats`` returns."""
    filters = {name: _filter_block() for name in FILTER_NAMES}
    return {
        "api": {
            "service": "bloom-filter-log-membership",
            "uptime_seconds": 12.3,
            "filters": filters,
            "totals": {
                "elements_added": 4800,
                "adds_total": 4800,
                "queries_total": 256,
                "memory_bytes": 4 * 1_198_136,
                "memory_mb": 4.571,
            },
        },
        "pipeline": {
            **{name: _pipeline_block() for name in FILTER_NAMES},
            "_totals": {
                "storage_rows": 120,
                "lookups": 80,
                "bloom_negatives": 40,
                "storage_skipped_pct": 50.0,
                "storage_hits": 32,
                "false_positives": 8,
                "observed_fp_rate": 0.2,
                "fallback_lookups": 0,
                "rotations_triggered": 0,
            },
        },
        "sessions": {
            "filter": {
                "elements_added": 1200,
                "capacity": 1_000_000,
                "slice_count": 1,
                "rotations": 0,
                "memory_bytes": 1_771_672,
                "memory_mb": 1.69,
                "estimated_fp_rate": 0.000002,
                "target_fp_rate": 0.01,
            },
            "memory_under_2mb": True,
            "pipeline": _pipeline_block(),
            "ops": {"adds_total": 1200, "queries_total": 64},
        },
        "error": None,
    }


# --------------------------------------------------------------------------- #
# fixtures                                                                    #
# --------------------------------------------------------------------------- #


@pytest.fixture
def dashboard_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Park the tick cadence at 100 ms with the settings cache cleared.

    Cleared again on the way out so later tests rebuild settings from the
    restored environment (same convention as ``tmp_data_dir`` in conftest).
    """
    monkeypatch.setenv("DASHBOARD_REFRESH_MS", str(REFRESH_MS))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def dashboard_client(
    dashboard_env: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """TestClient over the dashboard app with the stats fetch canned.

    ``fetch_all_stats`` is patched BEFORE the client (and therefore the
    lifespan + poll loop) starts — see the module docstring for why the
    ordering is load-bearing.
    """

    async def canned_fetch(settings) -> dict:  # noqa: ANN001 — fixture stub
        return canned_payload()

    monkeypatch.setattr(dashboard, "fetch_all_stats", canned_fetch)
    with TestClient(dashboard.app) as client:
        yield client


# --------------------------------------------------------------------------- #
# 1. GET / serves the page with the stable ids + vendored Chart.js reference. #
# --------------------------------------------------------------------------- #


def test_index_serves_dashboard_page(dashboard_client: TestClient) -> None:
    """``GET /`` → 200 HTML containing the chart canvases, cards, and assets."""
    r = dashboard_client.get("/")
    assert r.status_code == 200, r.text
    assert "text/html" in r.headers["content-type"]
    for needle in (
        'id="fp-chart"',
        'id="mem-chart"',
        "card-error_logs",
        "card-sessions",
        "/static/chart.umd.min.js",
        "/static/dashboard.js",
        "/static/dashboard.css",
        'id="conn-pill"',
        'id="pipeline-strip"',
    ):
        assert needle in r.text, f"dashboard HTML is missing {needle!r}"


# --------------------------------------------------------------------------- #
# 2 + 3. Static assets serve — and Chart.js is the genuine vendored bundle.   #
# --------------------------------------------------------------------------- #


def test_vendored_chartjs_serves(dashboard_client: TestClient) -> None:
    """The vendored Chart.js bundle is real (~200 KB of JS), not a stub page."""
    r = dashboard_client.get("/static/chart.umd.min.js")
    assert r.status_code == 200
    assert "javascript" in r.headers["content-type"]
    assert len(r.content) > 100_000, (
        f"chart.umd.min.js is only {len(r.content)} bytes — looks like a stub "
        "or an HTML error page, not the vendored Chart.js 4.4.1 bundle"
    )


def test_dashboard_assets_serve(dashboard_client: TestClient) -> None:
    """``dashboard.js`` and ``dashboard.css`` both serve 200."""
    js = dashboard_client.get("/static/dashboard.js")
    assert js.status_code == 200
    assert "/ws" in js.text  # the client actually wires the live feed
    css = dashboard_client.get("/static/dashboard.css")
    assert css.status_code == 200


# --------------------------------------------------------------------------- #
# 4. Health probe.                                                            #
# --------------------------------------------------------------------------- #


def test_health(dashboard_client: TestClient) -> None:
    """``GET /health`` → exactly the compose-healthcheck body."""
    r = dashboard_client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy"}


# --------------------------------------------------------------------------- #
# 5. WS /ws: an immediate, fully-shaped tick on connect.                      #
# --------------------------------------------------------------------------- #


def test_ws_immediate_tick_on_connect(dashboard_client: TestClient) -> None:
    """A fresh connection receives a tick at once — no waiting for the loop."""
    with dashboard_client.websocket_connect("/ws") as ws:
        tick = ws.receive_json()
    assert tick["type"] == "tick"
    assert tick["refresh_ms"] == REFRESH_MS
    assert isinstance(tick["ts"], float)
    assert tick["error"] is None
    assert sorted(tick["api"]["filters"]) == sorted(FILTER_NAMES)
    assert "_totals" in tick["pipeline"]
    assert tick["sessions"]["memory_under_2mb"] is True


# --------------------------------------------------------------------------- #
# 6. WS /ws: the poll loop keeps broadcasting at the configured cadence.      #
# --------------------------------------------------------------------------- #


def test_ws_periodic_ticks_arrive(dashboard_client: TestClient) -> None:
    """Staying connected ~0.35s (≥3 intervals at 100 ms) yields MORE ticks.

    The sleep lets loop broadcasts queue on the test session, so the second
    ``receive_json`` returns a tick that was pushed by the background loop,
    not by the connect handshake — proving the periodic fan-out works.
    """
    with dashboard_client.websocket_connect("/ws") as ws:
        first = ws.receive_json()
        assert first["type"] == "tick"
        time.sleep(0.35)
        second = ws.receive_json()
    assert second["type"] == "tick"
    assert second["refresh_ms"] == REFRESH_MS
    assert second["ts"] >= first["ts"]


# --------------------------------------------------------------------------- #
# 7. Error path: a raising fetch still produces (error-shaped) ticks.         #
# --------------------------------------------------------------------------- #


def test_ws_error_tick_when_fetch_raises(
    dashboard_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tick loop survives a fetch that RAISES (not just one that errors).

    ``fetch_all_stats`` normally converts its own failures into the
    error-shaped dict, but it is a documented monkeypatch seam — so the
    broadcast path wraps it (``_safe_fetch``) and degrades any exception to
    ``{"api": None, ..., "error": str(exc)}``. The client must keep
    receiving ticks through an API outage rather than a silently frozen
    page. Patched before ``TestClient`` enters, as always.
    """

    async def exploding_fetch(settings) -> dict:  # noqa: ANN001 — test stub
        raise RuntimeError("membership API is down")

    monkeypatch.setattr(dashboard, "fetch_all_stats", exploding_fetch)
    with TestClient(dashboard.app) as client:
        with client.websocket_connect("/ws") as ws:
            tick = ws.receive_json()
    assert tick["type"] == "tick"
    assert tick["api"] is None
    assert tick["pipeline"] is None
    assert tick["sessions"] is None
    assert tick["error"] is not None
    assert "membership API is down" in tick["error"]


# --------------------------------------------------------------------------- #
# 8. Proxies: forwarding, status relay, and 502 on connection failure.        #
# --------------------------------------------------------------------------- #


def test_proxy_routes_forward_to_api(
    dashboard_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each /proxy/* route forwards the validated body to the right API path."""
    calls: list[tuple[str, dict]] = []

    async def recording_proxy_post(url: str, payload: dict) -> tuple[int, dict]:
        calls.append((url, payload))
        return 200, {"relayed": True}

    monkeypatch.setattr(dashboard, "proxy_post", recording_proxy_post)

    r = dashboard_client.post(
        "/proxy/add", json={"log_type": "error_logs", "log_key": "k-add-1"}
    )
    assert r.status_code == 200 and r.json() == {"relayed": True}

    r = dashboard_client.post(
        "/proxy/query", json={"log_type": "access_logs", "log_key": "k-q-2"}
    )
    assert r.status_code == 200 and r.json() == {"relayed": True}

    r = dashboard_client.post(
        "/proxy/session-query", json={"session_id": "sess-77"}
    )
    assert r.status_code == 200 and r.json() == {"relayed": True}

    assert len(calls) == 3
    assert calls[0][0].endswith("/logs/add")
    assert calls[0][1] == {"log_type": "error_logs", "log_key": "k-add-1"}
    assert calls[1][0].endswith("/logs/query")
    assert calls[1][1] == {"log_type": "access_logs", "log_key": "k-q-2"}
    assert calls[2][0].endswith("/sessions/query")
    assert calls[2][1] == {"session_id": "sess-77"}


def test_proxy_relays_upstream_status_and_body(
    dashboard_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-2xx API response is relayed verbatim — not masked as a 502."""

    async def upstream_conflict(url: str, payload: dict) -> tuple[int, dict]:
        return 409, {"detail": "upstream said no"}

    monkeypatch.setattr(dashboard, "proxy_post", upstream_conflict)
    r = dashboard_client.post(
        "/proxy/query", json={"log_type": "error_logs", "log_key": "k"}
    )
    assert r.status_code == 409
    assert r.json() == {"detail": "upstream said no"}


def test_proxy_502_when_api_unreachable(
    dashboard_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transport-level failure becomes a 502 with a useful detail string."""

    async def refusing_proxy_post(url: str, payload: dict) -> tuple[int, dict]:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(dashboard, "proxy_post", refusing_proxy_post)
    r = dashboard_client.post(
        "/proxy/add", json={"log_type": "security_logs", "log_key": "k"}
    )
    assert r.status_code == 502
    assert "unreachable" in r.json()["detail"]


# --------------------------------------------------------------------------- #
# 9. Body validation happens dashboard-side (local Literal → 422, no relay).  #
# --------------------------------------------------------------------------- #


def test_proxy_validates_bodies_locally(
    dashboard_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bad bodies 422 locally and never reach (or need) the membership API."""
    calls: list[str] = []

    async def recording_proxy_post(url: str, payload: dict) -> tuple[int, dict]:
        calls.append(url)
        return 200, {}

    monkeypatch.setattr(dashboard, "proxy_post", recording_proxy_post)

    # Unknown log_type → the duplicated Literal rejects it dashboard-side.
    r = dashboard_client.post(
        "/proxy/add", json={"log_type": "made_up_logs", "log_key": "k"}
    )
    assert r.status_code == 422

    # Empty key / id → min_length=1 mirrors the API's contract.
    r = dashboard_client.post(
        "/proxy/query", json={"log_type": "error_logs", "log_key": ""}
    )
    assert r.status_code == 422
    r = dashboard_client.post("/proxy/session-query", json={"session_id": ""})
    assert r.status_code == 422

    assert calls == [], "invalid bodies must never be forwarded to the API"
