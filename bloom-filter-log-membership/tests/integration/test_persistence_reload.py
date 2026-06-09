"""Integration tests for persistence across app lifecycles (C8).

Two behaviors keep filters durable, and each gets a test over the real app:

* the **shutdown save** — leaving the lifespan writes a final snapshot, and
  a brand-new app lifecycle against the same DATA_DIR adopts it, so every
  key added before a restart still answers ``"probably_exists"`` after it;
* the **periodic snapshot task** — with the interval shrunk to 0.2s the
  background loop writes a snapshot file while the app is still up, proving
  durability does not depend on a clean shutdown.

Both tests drive ``TestClient(app)`` contexts by hand (the ``client``
fixture is one lifecycle; these tests need precise control over lifecycle
boundaries) on top of the ``api_env`` isolation fixture.
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.settings import get_settings


def test_keys_survive_app_restart(api_env: Path) -> None:
    """Add 50 keys, restart the app, and every one is still answerable."""
    from src.api import app

    keys = [f"persist-{i}" for i in range(50)]

    # Lifecycle 1: add the keys, then shut down (final save_all runs).
    with TestClient(app) as client:
        for key in keys:
            response = client.post(
                "/logs/add", json={"log_type": "error_logs", "log_key": key}
            )
            assert response.status_code == 200
    snapshot = api_env / "error_logs.bloom"
    assert snapshot.exists(), "shutdown did not write the final snapshot"

    # Lifecycle 2: same DATA_DIR — startup reload adopts the snapshot.
    with TestClient(app) as client:
        for key in keys:
            body = client.post(
                "/logs/query", json={"log_type": "error_logs", "log_key": key}
            ).json()
            assert body["might_exist"] is True, f"{key} lost across restart"
            assert body["confidence"] == "probably_exists"

        stats = client.get("/stats").json()
        assert stats["filters"]["error_logs"]["elements_added"] == 50


def test_snapshot_loop_writes_while_app_is_up(
    api_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With a 0.2s interval the background task snapshots without a shutdown."""
    monkeypatch.setenv("SNAPSHOT_INTERVAL_SECONDS", "0.2")
    get_settings.cache_clear()  # the override must beat api_env's cached 3600

    from src.api import app

    snapshot = api_env / "error_logs.bloom"
    with TestClient(app) as client:
        response = client.post(
            "/logs/add", json={"log_type": "error_logs", "log_key": "snap-1"}
        )
        assert response.status_code == 200

        # The loop should fire at ~0.2s; poll generously for slow CI rather
        # than sleeping a fixed amount. The assertion runs BEFORE the
        # context exits, so a hit here can only come from the background
        # task — the shutdown save has not happened yet.
        deadline = time.monotonic() + 5.0
        while not snapshot.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert snapshot.exists(), (
            "periodic snapshot task never wrote error_logs.bloom within 5s "
            "of a 0.2s interval"
        )
