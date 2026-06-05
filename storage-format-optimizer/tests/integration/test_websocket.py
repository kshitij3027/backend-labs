"""Integration tests for the live-metrics WebSocket stream (Commit 21).

Exercises the ``/ws`` endpoint end-to-end against the wired FastAPI app:

* the **immediate** tick a freshly connected dashboard receives (so it paints
  without waiting a full broadcast interval) — see ``ws_metrics`` /
  ``ConnectionManager.send_personal`` in :mod:`src.main` / :mod:`src.websocket`;
* the **periodic** tick the background ``_broadcast_loop`` fans out every
  ``ws_push_interval_seconds``;
* that a tick's payload reflects real ingested state (storage / format
  distribution / per-tenant rollup folded from the manifest in ``_build_tick``);
* and that a **dead** client is pruned without breaking its peers.

A LOCAL ``ws_client`` fixture (the shared ``client`` fixture in
``tests/conftest.py`` is left untouched) pins ``WS_PUSH_INTERVAL_SECONDS`` to
0.1s so a periodic tick arrives fast and all waits stay implicit:
``starlette``'s ``TestClient`` ``receive_json`` blocks until the next frame, and
the 0.1s loop bounds that wait — no arbitrary ``sleep`` is needed.
"""
from __future__ import annotations

import pytest

# Top-level keys every tick document carries.
_TICK_TOP_KEYS = {"stats", "series", "tenants", "migrations", "indexes", "tiers"}
# Top-level keys of the embedded metrics snapshot (Metrics.snapshot()).
_STATS_KEYS = {"storage", "formats", "performance", "migrations", "ingest"}


@pytest.fixture
def ws_client(tmp_path, monkeypatch):
    """Yield a ``TestClient`` with a fast WS push interval (isolated data dir).

    Mirrors the shared ``client`` fixture but additionally pins
    ``WS_PUSH_INTERVAL_SECONDS`` to 0.1s so the background broadcast loop emits a
    periodic tick quickly. The ``get_settings`` LRU cache is cleared on entry (so
    the env overrides take effect) and on exit (so a later test rebuilds fresh).
    Entering the ``TestClient`` context runs the app lifespan, which starts the
    broadcast loop; exiting it tears the loop down.
    """
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("MIGRATION_INTERVAL_SECONDS", "3600")  # no auto-migrate
    monkeypatch.setenv("WS_PUSH_INTERVAL_SECONDS", "0.1")  # fast periodic ticks
    from src.settings import get_settings

    get_settings.cache_clear()
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c
    get_settings.cache_clear()


def _assert_valid_tick(msg: dict) -> None:
    """Assert ``msg`` is a well-formed tick document with the full payload shape."""
    assert msg["type"] == "tick"
    # Every documented top-level key is present.
    assert _TICK_TOP_KEYS <= set(msg), f"missing tick keys: {_TICK_TOP_KEYS - set(msg)}"
    # The embedded metrics snapshot has its five canonical sections.
    assert _STATS_KEYS <= set(msg["stats"]), (
        f"missing stats keys: {_STATS_KEYS - set(msg['stats'])}"
    )
    # Tier rollup always carries the three canonical buckets.
    assert {"hot", "warm", "cold"} <= set(msg["tiers"])


# --------------------------------------------------------------------------- #
# 1. Immediate tick on connect, with the full payload shape.
# --------------------------------------------------------------------------- #
def test_immediate_tick_on_connect(ws_client):
    """Connecting yields an immediate, well-shaped ``type=="tick"`` frame."""
    with ws_client.websocket_connect("/ws") as ws:
        msg = ws.receive_json()

    _assert_valid_tick(msg)
    # indexes rollup exposes the indexed-column total.
    assert "columns_indexed" in msg["indexes"]


# --------------------------------------------------------------------------- #
# 2. A second, periodic tick arrives from the background loop.
# --------------------------------------------------------------------------- #
def test_periodic_tick_arrives(ws_client):
    """After the immediate frame, the 0.1s broadcast loop delivers another tick.

    ``receive_json`` blocks until the next frame; with the push interval pinned
    to 0.1s the background loop bounds that wait, so no explicit sleep is needed.
    """
    with ws_client.websocket_connect("/ws") as ws:
        first = ws.receive_json()  # immediate (send_personal)
        second = ws.receive_json()  # periodic (broadcast loop)

    _assert_valid_tick(first)
    _assert_valid_tick(second)
    assert second["type"] == "tick"


# --------------------------------------------------------------------------- #
# 3. A tick reflects ingested data.
# --------------------------------------------------------------------------- #
def test_tick_reflects_ingested_data(ws_client):
    """Data ingested before connecting shows up in the very first tick."""
    body = {
        "tenant": "acme",
        "entries": [{"ts": 3600.0, "fields": {"u": "a"}} for _ in range(5)],
    }
    r = ws_client.post("/api/ingest", json=body)
    assert r.status_code == 200, r.text

    with ws_client.websocket_connect("/ws") as ws:
        tick = ws.receive_json()

    _assert_valid_tick(tick)
    # Lifetime ingest total reflects the 5 rows.
    assert tick["stats"]["ingest"]["total_entries"] >= 5
    # New partitions are written ROW-first -> at least one row partition.
    assert tick["stats"]["formats"]["distribution"]["row"] >= 1
    # The tenant rollup includes the tenant we ingested for.
    assert "acme" in tick["tenants"]


# --------------------------------------------------------------------------- #
# 4. A dead client is pruned without breaking its peers.
# --------------------------------------------------------------------------- #
def test_dead_client_does_not_break_peers(ws_client):
    """Closing one socket leaves a second receiving valid periodic ticks.

    Two clients connect; the first is closed (so the next broadcast's send to it
    raises and the manager prunes it). The second must still receive a valid tick
    from the very next periodic broadcast — proving one dead client never aborts
    the fan-out to the survivors. Waits stay bounded by the 0.1s loop.
    """
    with ws_client.websocket_connect("/ws") as ws2:
        # Drain ws2's immediate frame so subsequent receives are periodic ticks.
        ws2.receive_json()

        # Open a second client, take its immediate frame, then close it so the
        # next broadcast finds it dead.
        with ws_client.websocket_connect("/ws") as ws1:
            ws1.receive_json()
        # ws1 is now closed (context exited).

        # The survivor keeps getting periodic ticks; the pruned peer is invisible
        # to it. A couple of receives bounds out any frame already in flight when
        # ws1 closed and confirms the loop is still healthy afterwards.
        tick = ws2.receive_json()
        _assert_valid_tick(tick)
        tick_again = ws2.receive_json()
        _assert_valid_tick(tick_again)
