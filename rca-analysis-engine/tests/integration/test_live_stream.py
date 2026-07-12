"""Integration tests for the C8 background live-stream loop.

The loop is OFF by default and must never run under the injected-runtime fixtures. These
tests cover:

* **loop body (robust / deterministic)** — driving :func:`src.main._live_stream_loop`
  directly with a recording manager and :func:`asyncio.wait_for`, asserting an
  ``incident_update`` frame is broadcast, recorded in the shared history, and that a single
  bad tick is survived rather than killing the stream;
* **gating** — with ``live_stream_enabled`` false (default) the lifespan starts no task and
  a connected client receives no unsolicited frame; with it true the lifespan starts the
  task and frames flow to a ``/ws`` client.

The direct loop-body tests are the reliable ones (no TestClient timing); the lifespan
tests are kept robust with generous waits and tolerate one-or-more frames.
"""

import asyncio
import time
from contextlib import suppress

from fastapi.testclient import TestClient

from src.analysis import RCAAnalyzer
from src.api import create_app
from src.config import Settings, get_settings
from src.main import Runtime, _live_stream_loop


class _RecordingManager:
    """A ConnectionManager stand-in: records broadcast frames and signals the first."""

    def __init__(self) -> None:
        self.frames: list[dict] = []
        self.received = asyncio.Event()

    async def broadcast(self, message: dict) -> None:
        self.frames.append(message)
        self.received.set()


def _runtime(settings: Settings, manager: _RecordingManager) -> Runtime:
    return Runtime(
        settings=settings, analyzer=RCAAnalyzer(settings), connection_manager=manager
    )


# --- Loop body (direct, deterministic) -------------------------------------------


async def test_live_stream_loop_broadcasts_incident_update():
    settings = Settings(_env_file=None, live_stream_enabled=True, live_stream_interval=0.1)
    manager = _RecordingManager()
    runtime = _runtime(settings, manager)

    task = asyncio.create_task(_live_stream_loop(runtime))
    try:
        await asyncio.wait_for(manager.received.wait(), timeout=3.0)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    assert manager.frames
    frame = manager.frames[0]
    assert frame["type"] == "incident_update"
    assert frame["data"]["incident_id"].startswith("live-")
    # The snapshot was also recorded in the shared bounded history.
    assert runtime.analyzer.incident_history


async def test_live_stream_loop_survives_a_bad_tick(monkeypatch):
    settings = Settings(_env_file=None, live_stream_enabled=True, live_stream_interval=0.1)
    manager = _RecordingManager()
    runtime = _runtime(settings, manager)

    from src import generators

    calls = {"n": 0}
    real_generate = generators.generate_incident

    def flaky_generate(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom tick")  # first tick blows up
        return real_generate(*args, **kwargs)

    monkeypatch.setattr(generators, "generate_incident", flaky_generate)

    task = asyncio.create_task(_live_stream_loop(runtime))
    try:
        await asyncio.wait_for(manager.received.wait(), timeout=3.0)
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    # The first tick raised and was swallowed; the loop continued to a healthy broadcast.
    assert calls["n"] >= 2
    assert manager.frames
    assert manager.frames[0]["type"] == "incident_update"


async def test_live_stream_loop_stops_on_cancel():
    # Cancellation must propagate (except Exception never swallows CancelledError), so the
    # awaited task finishes promptly rather than hanging.
    settings = Settings(_env_file=None, live_stream_enabled=True, live_stream_interval=0.1)
    manager = _RecordingManager()
    runtime = _runtime(settings, manager)

    task = asyncio.create_task(_live_stream_loop(runtime))
    await asyncio.wait_for(manager.received.wait(), timeout=3.0)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=3.0)
    assert task.cancelled() or task.done()


# --- Gating via the real lifespan ------------------------------------------------


def test_live_stream_disabled_by_default(monkeypatch):
    monkeypatch.delenv("LIVE_STREAM_ENABLED", raising=False)
    get_settings.cache_clear()
    try:
        app = create_app()  # lifespan path (no injected runtime)
        with TestClient(app) as client:
            # Gate: the loop is NOT started when the master switch is off.
            assert app.state.runtime.live_task is None
            # And a connected client gets no unsolicited push — only our ping/pong.
            with client.websocket_connect("/ws") as ws:
                ws.send_text("ping")
                assert ws.receive_text() == "pong"
    finally:
        get_settings.cache_clear()


def test_live_stream_enabled_starts_task_and_broadcasts(monkeypatch):
    monkeypatch.setenv("LIVE_STREAM_ENABLED", "true")
    monkeypatch.setenv("LIVE_STREAM_INTERVAL", "0.1")
    monkeypatch.setenv("LIVE_STREAM_SEED", "0")
    get_settings.cache_clear()
    try:
        app = create_app()
        with TestClient(app) as client:
            # Gate: the loop IS started when enabled.
            assert app.state.runtime.live_task is not None
            # Confirm the loop is actually producing incidents (bounded, non-blocking) BEFORE
            # blocking on a websocket frame — a dead loop fails fast here instead of hanging.
            deadline = time.time() + 3.0
            while time.time() < deadline and not client.get("/api/incidents").json():
                time.sleep(0.05)
            incidents = client.get("/api/incidents").json()
            assert incidents, "live loop should have recorded a live incident"
            assert incidents[0]["incident_id"].startswith("live-")
            # The loop is live and broadcasting; a connected client receives an incident_update.
            with client.websocket_connect("/ws") as ws:
                frame = ws.receive_json()
                assert frame["type"] == "incident_update"
                assert frame["data"]["incident_id"].startswith("live-")
    finally:
        get_settings.cache_clear()
